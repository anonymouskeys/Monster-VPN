package com.v2ray.ang.service

import android.content.Context
import com.v2ray.ang.core.CoreConfigManager
import com.v2ray.ang.core.CoreNativeManager
import com.v2ray.ang.dpi.ByeDpiManager
import com.v2ray.ang.dto.RealPingEvent
import com.v2ray.ang.enums.EConfigType
import com.v2ray.ang.extension.isComplexType
import com.v2ray.ang.extension.isNotNullEmpty
import com.v2ray.ang.handler.MmkvManager
import com.v2ray.ang.handler.SettingsManager
import com.v2ray.ang.handler.SpeedtestManager
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext

/**
 * Runs one cancellable batch of real-ping tests.
 *
 * DPI testing uses a two-stage scheduler:
 * 1. a fast parallel pass for every profile;
 * 2. a narrow retry pass only for temporary transport failures.
 *
 * Retrying temporary failures immediately in the same busy pool caused a feedback loop: every
 * timeout created another native Xray instance while the first one was still closing, producing
 * EOF/closed-pipe storms and overwriting previously valid delays with -1. The second stage lets
 * the native runtime drain before rechecking doubtful profiles at low concurrency.
 */
class RealPingWorkerService(
    private val context: Context,
    private val guids: List<String>,
    private val onEvent: (RealPingEvent) -> Unit = {}
) {
    private val job = SupervisorJob()
    private val scope = CoroutineScope(job + Dispatchers.IO + CoroutineName("RealPingBatchWorker"))
    private val completedCount = AtomicInteger(0)

    private data class PingResult(
        val guid: String,
        val delayMillis: Long,
        val transientFailure: Boolean
    )

    fun start() {
        scope.launch {
            try {
                runBatch()
                onEvent(RealPingEvent.Finish("0"))
            } catch (_: CancellationException) {
                onEvent(RealPingEvent.Finish("-1"))
            }
        }
    }

    fun cancel() {
        job.cancel()
    }

    private suspend fun runBatch() = coroutineScope {
        if (guids.isEmpty()) return@coroutineScope

        val dpiRunning = ByeDpiManager.isRunning()
        val configuredConcurrency = SettingsManager.getRealPingConcurrency()
        val firstPassConcurrency = if (dpiRunning) {
            configuredConcurrency.coerceIn(4, 8)
        } else {
            configuredConcurrency.coerceIn(1, 64)
        }.coerceAtMost(guids.size)

        if (dpiRunning) delay(300L)

        val transientQueue = ConcurrentLinkedQueue<String>()
        val nextIndex = AtomicInteger(0)

        repeat(firstPassConcurrency) { workerIndex ->
            launch {
                if (dpiRunning && workerIndex > 0) {
                    delay((workerIndex * 45L).coerceAtMost(280L))
                }
                while (true) {
                    coroutineContext.ensureActive()
                    val index = nextIndex.getAndIncrement()
                    if (index >= guids.size) break

                    val result = testProfile(
                        guid = guids[index],
                        dpiRunning = dpiRunning,
                        testUrl = SettingsManager.getDelayTestUrl()
                    )
                    if (result.transientFailure && dpiRunning) {
                        transientQueue.add(result.guid)
                    } else {
                        publishFinal(result)
                    }
                }
            }
        }

        // Wait until every first-pass worker above has completed before entering the retry stage.
        // coroutineScope waits for child jobs only when this block is about to return, therefore
        // use small child scopes for explicit stage barriers.
        while (nextIndex.get() < guids.size || completedCount.get() + transientQueue.size < guids.size) {
            coroutineContext.ensureActive()
            delay(50L)
        }

        if (transientQueue.isEmpty()) return@coroutineScope

        // Give ciadpi and the native Xray instances time to close sockets from the fast pass.
        delay(600L)

        val retryGuids = transientQueue.toList()
        val retryIndex = AtomicInteger(0)
        val retryConcurrency = minOf(2, retryGuids.size)
        repeat(retryConcurrency) { workerIndex ->
            launch {
                if (workerIndex > 0) delay(160L)
                while (true) {
                    coroutineContext.ensureActive()
                    val index = retryIndex.getAndIncrement()
                    if (index >= retryGuids.size) break

                    val result = testProfile(
                        guid = retryGuids[index],
                        dpiRunning = true,
                        testUrl = SettingsManager.getDelayTestUrl(true)
                    )
                    publishFinal(result)
                    if (result.transientFailure) delay(120L)
                }
            }
        }
    }

    private fun publishFinal(result: PingResult) {
        onEvent(
            RealPingEvent.Result(
                guid = result.guid,
                delayMillis = result.delayMillis,
                transientFailure = result.transientFailure
            )
        )
        val done = completedCount.incrementAndGet()
        onEvent(RealPingEvent.Progress("$done / ${guids.size}"))
    }

    private suspend fun testProfile(
        guid: String,
        dpiRunning: Boolean,
        testUrl: String
    ): PingResult {
        val permanentFailure = PingResult(guid, -1L, transientFailure = false)
        val config = MmkvManager.decodeServerConfig(guid) ?: return permanentFailure

        // A direct TCP pre-check bypasses ByeDPI. Keep it only for normal mode, otherwise it can
        // reject a profile that succeeds through the actual DPI chain.
        if (!dpiRunning
            && !config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.startsWith("h3") != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val tcpTime = SpeedtestManager.socketConnectTime(
                config.server.orEmpty(),
                config.serverPort.orEmpty().toInt(),
                3000
            )
            if (tcpTime <= -1L) return permanentFailure
        }

        val configResult = CoreConfigManager.getV2rayConfig4Speedtest(context, guid)
        if (!configResult.status) return permanentFailure

        coroutineContext.ensureActive()
        val measurement = CoreNativeManager.measureOutboundDelayDetailed(configResult.content, testUrl)
        return PingResult(
            guid = guid,
            delayMillis = measurement.delayMillis,
            transientFailure = measurement.delayMillis < 0L && measurement.isTransient
        )
    }
}
