package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type rpcModelRouteRequest struct {
	pluginapi.ModelRouteRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcExecutorRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type executorStreamResponse struct {
	Headers http.Header                     `json:"headers,omitempty"`
	Chunks  []pluginapi.ExecutorStreamChunk `json:"chunks,omitempty"`
}

type rpcStreamEmitRequest struct {
	StreamID    string           `json:"stream_id"`
	Payload     []byte           `json:"payload,omitempty"`
	Error       string           `json:"error,omitempty"`
	ErrorDetail *pluginabi.Error `json:"error_detail,omitempty"`
}

type rpcStreamCloseRequest struct {
	StreamID    string           `json:"stream_id"`
	Error       string           `json:"error,omitempty"`
	ErrorDetail *pluginabi.Error `json:"error_detail,omitempty"`
}

type transparentResult struct {
	Payload   []byte
	Headers   http.Header
	Chunks    []pluginapi.ExecutorStreamChunk
	Available bool
}

type transparentStreamAttempt struct {
	Class     string
	Verdict   QualityVerdict
	Err       error
	Buffered  [][]byte
	Emitted   bool
	Available bool
}

type streamReadEvent struct {
	Response pluginapi.HostModelStreamReadResponse
	Err      error
}

func (p *Plugin) RouteModel(request []byte) ([]byte, error) {
	var req rpcModelRouteRequest
	if len(request) == 0 || json.Unmarshal(request, &req) != nil {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false, Reason: "invalid_route_request"})
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	if !policy.TransparentGuard {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false, Reason: "transparent_guard_disabled"})
	}
	if isProbeHeader(req.Headers) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false, Reason: "internal_probe_request"})
	}
	requestedModel := firstNonEmpty(req.RequestedModel, asString(req.Metadata["model"]), modelFromRequestBody(req.Body))
	if !p.isGrokRouteModel(requestedModel) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false, Reason: "not_grok_model"})
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(req.Query.Get("alt"))), "compact") || isResponsesCompactionRequest(req.Body) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false, Reason: "responses_compaction"})
	}
	return okEnvelope(pluginapi.ModelRouteResponse{
		Handled:    true,
		TargetKind: pluginapi.ModelRouteTargetSelf,
		Target:     pluginID,
		Reason:     "grok_quality_guard_transparent",
	})
}

func (p *Plugin) Execute(request []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if len(request) == 0 || json.Unmarshal(request, &req) != nil {
		return errorEnvelope("invalid_request", "decode executor request", http.StatusBadRequest), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	result, errExecute := p.transparentExecute(ctx, req)
	if errExecute != nil {
		return errorEnvelope("quality_degraded", "no healthy Grok account produced a valid response", http.StatusServiceUnavailable), nil
	}
	return okEnvelope(pluginapi.ExecutorResponse{Payload: result.Payload, Headers: result.Headers})
}

func (p *Plugin) ExecuteStream(request []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if len(request) == 0 || json.Unmarshal(request, &req) != nil {
		return errorEnvelope("invalid_request", "decode executor request", http.StatusBadRequest), nil
	}
	streamID := strings.TrimSpace(req.StreamID)
	if streamID == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		result, errExecute := p.transparentExecuteBufferedStream(ctx, req)
		if errExecute != nil {
			return errorEnvelope("quality_degraded", "no healthy Grok account produced a valid response", http.StatusServiceUnavailable), nil
		}
		return okEnvelope(executorStreamResponse{Headers: result.Headers, Chunks: result.Chunks})
	}
	go p.runTransparentStreamBridge(context.Background(), req, streamID)
	return okEnvelope(executorStreamResponse{Headers: sseHeaders()})
}

func (p *Plugin) transparentExecute(ctx context.Context, req rpcExecutorRequest) (transparentResult, error) {
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	if !policy.TransparentGuard {
		return transparentResult{}, fmt.Errorf("transparent guard is disabled")
	}
	body, sourceFormat, errRequest := executorRequestInput(req)
	if errRequest != nil {
		return transparentResult{}, errRequest
	}
	body = ensureTransparentReasoningEvidence(body, sourceFormat, executionModel(req, body))
	replayable := safeReplayRequest(body, sourceFormat, firstNonEmpty(req.Model, req.Alt))
	selection, errSelection := p.newTransparentSelection(req, body, sourceFormat)
	if errSelection != nil {
		return transparentResult{}, errSelection
	}
	maxAttempts := transparentMaxAttempts(policy, replayable)
	attempts := 0
	last := transparentResult{}
candidateLoop:
	for {
		attempt, ok := selection.Next(ctx)
		if !ok {
			break
		}
		attemptID := attempt.Account.ID
		source := attempt.Source
		rotated := false
		for {
			if attempts >= maxAttempts {
				attempt.Release()
				break candidateLoop
			}
			account, raw, ok := p.executableAuth(ctx, attemptID)
			if !ok {
				attempt.Release()
				p.refreshSelectionAccount(attemptID)
				break
			}
			attempts++
			execution := buildHostExecution(req, body, sourceFormat, account.ID, authRawString(raw, "proxy_url"), false)
			result, verdict, errAttempt := p.executeTransparentOnce(ctx, execution, req.HostCallbackID, body, policy.Quality)
			if errAttempt == nil {
				selection.MarkSuccess(account.ID, source)
				attempt.Release()
				return result, nil
			}
			class := verdict.Class
			if result.Available {
				last = result
			}
			_ = p.recordAttemptError(account.ID, class, errAttempt)
			selection.MarkFailure(account.ID, source, class)
			if replayable && attempts < maxAttempts && !rotated && shouldRotateTransparentFailure(selection.totalCandidates, class) {
				didRotate, errRotate := p.rotateAfterFailure(ctx, account.ID, class)
				if errRotate == nil && didRotate {
					rotated = true
					continue
				}
			}
			p.applyTransparentFailureVerdict(ctx, account.ID, verdict, class, errAttempt, req.HostCallbackID)
			attempt.Release()
			break
		}
	}
	if policy.Quality.OnExhausted == "fail_open" && last.Available {
		return last, nil
	}
	return transparentResult{}, fmt.Errorf("no healthy Grok account produced a valid response")
}

func (p *Plugin) transparentExecuteBufferedStream(ctx context.Context, req rpcExecutorRequest) (transparentResult, error) {
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	if !policy.TransparentGuard {
		return transparentResult{}, fmt.Errorf("transparent guard is disabled")
	}
	body, sourceFormat, errRequest := executorRequestInput(req)
	if errRequest != nil {
		return transparentResult{}, errRequest
	}
	body = ensureTransparentReasoningEvidence(body, sourceFormat, executionModel(req, body))
	replayable := safeReplayRequest(body, sourceFormat, firstNonEmpty(req.Model, req.Alt))
	selection, errSelection := p.newTransparentSelection(req, body, sourceFormat)
	if errSelection != nil {
		return transparentResult{}, errSelection
	}
	maxAttempts := transparentMaxAttempts(policy, replayable)
	attempts := 0
	last := transparentResult{}
candidateLoop:
	for {
		attempt, ok := selection.Next(ctx)
		if !ok {
			break
		}
		attemptID := attempt.Account.ID
		source := attempt.Source
		rotated := false
		for {
			if attempts >= maxAttempts {
				attempt.Release()
				break candidateLoop
			}
			account, raw, ok := p.executableAuth(ctx, attemptID)
			if !ok {
				attempt.Release()
				p.refreshSelectionAccount(attemptID)
				break
			}
			attempts++
			execution := buildHostExecution(req, body, sourceFormat, account.ID, authRawString(raw, "proxy_url"), true)
			result, verdict, errAttempt := p.executeTransparentBufferedAttempt(ctx, execution, req.HostCallbackID, body, policy.Quality)
			if errAttempt == nil {
				selection.MarkSuccess(account.ID, source)
				attempt.Release()
				return result, nil
			}
			class := verdict.Class
			if result.Available {
				last = result
			}
			_ = p.recordAttemptError(account.ID, class, errAttempt)
			selection.MarkFailure(account.ID, source, class)
			if replayable && attempts < maxAttempts && !rotated && shouldRotateTransparentFailure(selection.totalCandidates, class) {
				didRotate, errRotate := p.rotateAfterFailure(ctx, account.ID, class)
				if errRotate == nil && didRotate {
					rotated = true
					continue
				}
			}
			p.applyTransparentFailureVerdict(ctx, account.ID, verdict, class, errAttempt, req.HostCallbackID)
			attempt.Release()
			break
		}
	}
	if policy.Quality.OnExhausted == "fail_open" && last.Available {
		return last, nil
	}
	return transparentResult{}, fmt.Errorf("no healthy Grok account produced a valid response")
}

func (p *Plugin) executeTransparentOnce(ctx context.Context, execution pluginapi.HostModelExecutionRequest, callbackID string, requestBody []byte, cfg QualityConfig) (transparentResult, QualityVerdict, error) {
	execution.Stream = false
	startedAt := time.Now()
	response, errExecute := p.executeModel(ctx, execution, callbackID)
	if errExecute != nil {
		class := classifyTransportError(errExecute)
		return transparentResult{}, QualityVerdict{Class: class, Reason: sanitizeError(errExecute)}, errExecute
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		class, reason := classifyUpstreamStatus(response.StatusCode, response.Body)
		return transparentResult{}, QualityVerdict{Class: class, Reason: reason}, fmt.Errorf("%s", reason)
	}
	analyzer := NewStreamAnalyzerWithProtocolAt(execution.ExitProtocol, startedAt)
	analyzer.SetStreaming(false)
	analyzer.SetEncryptedConfig(cfg.MinEncryptedBytes, cfg.EncryptedBytesPerReasoningToken)
	analyzer.ObserveJSON(response.Body)
	sig := analyzer.Finalize(true)
	sig.DurationMS = time.Since(startedAt).Milliseconds()
	sig.GenerationMS = generationWindowMS(sig.DurationMS, sig.FirstTokenMS, sig.ReasoningTokens)
	sig.TPS = tps(sig.OutputTokens, sig.GenerationMS)
	verdict := classifyNonStreamResult(sig, cfg, reasoningRequiredForModel(execution.Model, requestBody), cfg.MinHoldOutputTokens)
	result := transparentResult{Payload: response.Body, Headers: response.Headers, Available: true}
	if verdict.Class == ClassNone {
		_ = p.ApplyVerdict(ctx, execution.AuthID, verdict, "transparent", callbackID)
		return result, verdict, nil
	}
	return result, verdict, fmt.Errorf("quality gate rejected response: %s", verdict.Class)
}

func (p *Plugin) executeTransparentBufferedAttempt(ctx context.Context, execution pluginapi.HostModelExecutionRequest, callbackID string, requestBody []byte, cfg QualityConfig) (transparentResult, QualityVerdict, error) {
	// The host returns the stream only after the first chunk is readable, so the
	// TPS clock must start before the call to include upstream TTFT.
	attemptStartedAt := time.Now()
	stream, errExecute := p.executeModelStream(ctx, execution, callbackID)
	if errExecute != nil {
		class := classifyTransportError(errExecute)
		return transparentResult{}, QualityVerdict{Class: class, Reason: sanitizeError(errExecute)}, errExecute
	}
	defer p.closeModelStream(stream.StreamID)
	if stream.StatusCode < 200 || stream.StatusCode >= 300 {
		class, reason := classifyUpstreamStatus(stream.StatusCode, p.readModelStreamErrorBody(ctx, stream.StreamID))
		return transparentResult{}, QualityVerdict{Class: class, Reason: reason}, fmt.Errorf("%s", reason)
	}
	analyzer := NewStreamAnalyzerWithProtocolAt(execution.ExitProtocol, attemptStartedAt)
	outputNormalizer := &sseOutputNormalizer{}
	chunks := make([]pluginapi.ExecutorStreamChunk, 0)
	var bufferBytes int64
	for {
		select {
		case <-ctx.Done():
			return transparentResult{}, QualityVerdict{Class: ClassProbeError, Reason: sanitizeError(ctx.Err())}, ctx.Err()
		default:
		}
		read, errRead := p.readModelStreamContext(ctx, stream.StreamID)
		if errRead != nil {
			class := classifyTransportError(errRead)
			return transparentResult{}, QualityVerdict{Class: class, Reason: sanitizeError(errRead)}, errRead
		}
		if len(read.Payload) > 0 {
			bufferBytes += int64(len(read.Payload))
			analyzer.ObserveChunk(read.Payload)
			for _, payload := range outputNormalizer.Feed(read.Payload) {
				chunks = append(chunks, pluginapi.ExecutorStreamChunk{Payload: payload})
			}
			if bufferBytes > cfg.HoldMaxBufferBytes {
				reason := fmt.Sprintf("transparent hold buffer exceeded %d bytes", cfg.HoldMaxBufferBytes)
				return transparentResult{}, QualityVerdict{Action: QualityActionWithhold, Class: ClassProbeError, Reason: reason}, fmt.Errorf("%s", reason)
			}
		}
		if read.Error != "" {
			errRead := fmt.Errorf("%s", read.Error)
			class := classifyTransportError(errRead)
			return transparentResult{}, QualityVerdict{Class: class, Reason: sanitizeError(errRead)}, errRead
		}
		if read.Done {
			break
		}
	}
	for _, payload := range outputNormalizer.Flush() {
		chunks = append(chunks, pluginapi.ExecutorStreamChunk{Payload: payload})
	}
	sig := analyzer.Finalize(true)
	verdict := classifyFinal(sig, cfg, reasoningRequiredForModel(execution.Model, requestBody), cfg.MinHoldOutputTokens, true)
	result := transparentResult{Headers: stream.Headers, Chunks: chunks, Available: true}
	if verdict.Class == ClassNone {
		_ = p.ApplyVerdict(ctx, execution.AuthID, verdict, "transparent", callbackID)
		return result, verdict, nil
	}
	return result, verdict, fmt.Errorf("quality gate rejected stream: %s", verdict.Class)
}

func (p *Plugin) runTransparentStreamBridge(ctx context.Context, req rpcExecutorRequest, streamID string) {
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	ctx = runCtx
	defer func() {
		if recovered := recover(); recovered != nil {
			p.closePluginStream(streamID, "plugin_stream_panic", "quality guard stream failed", http.StatusInternalServerError)
		}
	}()
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	if !policy.TransparentGuard {
		p.closePluginStream(streamID, "transparent_guard_disabled", "transparent guard is disabled", http.StatusServiceUnavailable)
		return
	}
	body, sourceFormat, errRequest := executorRequestInput(req)
	if errRequest != nil {
		p.closePluginStream(streamID, "invalid_request", "request body is empty", http.StatusBadRequest)
		return
	}
	body = ensureTransparentReasoningEvidence(body, sourceFormat, executionModel(req, body))
	replayable := safeReplayRequest(body, sourceFormat, firstNonEmpty(req.Model, req.Alt))
	selection, errSelection := p.newTransparentSelection(req, body, sourceFormat)
	if errSelection != nil {
		p.closePluginStream(streamID, "quality_degraded", "no healthy Grok account produced a valid response", http.StatusServiceUnavailable)
		return
	}
	maxAttempts := transparentMaxAttempts(policy, replayable)
	attempts := 0
	last := transparentStreamAttempt{}
candidateLoop:
	for {
		attempt, ok := selection.Next(ctx)
		if !ok {
			break
		}
		attemptID := attempt.Account.ID
		source := attempt.Source
		rotated := false
		for {
			if attempts >= maxAttempts {
				attempt.Release()
				break candidateLoop
			}
			account, raw, ok := p.executableAuth(ctx, attemptID)
			if !ok {
				attempt.Release()
				p.refreshSelectionAccount(attemptID)
				break
			}
			attempts++
			execution := buildHostExecution(req, body, sourceFormat, account.ID, authRawString(raw, "proxy_url"), true)
			outcome := p.executeTransparentStreamAttempt(ctx, execution, req.HostCallbackID, body, policy.Quality, streamID)
			if outcome.Emitted {
				if outcome.Err != nil {
					selection.MarkFailure(account.ID, source, outcome.Class)
					attempt.Release()
					p.closePluginStream(streamID, "upstream_stream_error", "upstream stream failed after delivery started", http.StatusBadGateway)
					return
				}
				selection.MarkSuccess(account.ID, source)
				attempt.Release()
				p.closePluginStream(streamID, "", "", 0)
				return
			}
			if outcome.Err == nil {
				selection.MarkSuccess(account.ID, source)
				attempt.Release()
				p.closePluginStream(streamID, "", "", 0)
				return
			}
			if ctx.Err() != nil {
				attempt.Release()
				p.closePluginStream(streamID, "", "", 0)
				return
			}
			class := outcome.Class
			if outcome.Verdict.Class != "" {
				class = outcome.Verdict.Class
			}
			if outcome.Available {
				last = outcome
			}
			_ = p.recordAttemptError(account.ID, class, outcome.Err)
			selection.MarkFailure(account.ID, source, class)
			if replayable && attempts < maxAttempts && !rotated && shouldRotateTransparentFailure(selection.totalCandidates, class) {
				didRotate, errRotate := p.rotateAfterFailure(ctx, account.ID, class)
				if errRotate == nil && didRotate {
					rotated = true
					continue
				}
			}
			p.applyTransparentFailureVerdict(ctx, account.ID, outcome.Verdict, class, outcome.Err, req.HostCallbackID)
			attempt.Release()
			break
		}
	}
	if policy.Quality.OnExhausted == "fail_open" && last.Available {
		for _, payload := range last.Buffered {
			if errEmit := p.emitPluginStreamChunk(streamID, payload); errEmit != nil {
				return
			}
		}
		p.closePluginStream(streamID, "", "", 0)
		return
	}
	p.closePluginStream(streamID, "quality_degraded", "no healthy Grok account produced a valid response", http.StatusServiceUnavailable)
}

func (p *Plugin) executeTransparentStreamAttempt(ctx context.Context, execution pluginapi.HostModelExecutionRequest, callbackID string, requestBody []byte, cfg QualityConfig, streamID string) transparentStreamAttempt {
	// The host returns the stream only after the first chunk is readable, so the
	// TPS clock must start before the call to include upstream TTFT.
	attemptStartedAt := time.Now()
	stream, errExecute := p.executeModelStream(ctx, execution, callbackID)
	if errExecute != nil {
		class := classifyTransportError(errExecute)
		return transparentStreamAttempt{Class: class, Verdict: QualityVerdict{Class: class, Reason: sanitizeError(errExecute)}, Err: errExecute}
	}
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	if stream.StatusCode < 200 || stream.StatusCode >= 300 {
		class, reason := classifyUpstreamStatus(stream.StatusCode, p.readModelStreamErrorBody(attemptCtx, stream.StreamID))
		cancelAttempt()
		_ = p.closeModelStream(stream.StreamID)
		return transparentStreamAttempt{Class: class, Verdict: QualityVerdict{Class: class, Reason: reason}, Err: fmt.Errorf("%s", reason)}
	}
	analyzer := NewStreamAnalyzerWithProtocolAt(execution.ExitProtocol, attemptStartedAt)
	analyzer.SetEncryptedConfig(cfg.MinEncryptedBytes, cfg.EncryptedBytesPerReasoningToken)
	readEvents := make(chan streamReadEvent, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer close(readEvents)
		for {
			read, errRead := p.readModelStream(context.Background(), stream.StreamID)
			event := streamReadEvent{Response: read, Err: errRead}
			select {
			case readEvents <- event:
			case <-attemptCtx.Done():
				return
			}
			if errRead != nil || read.Done {
				return
			}
		}
	}()
	defer func() {
		cancelAttempt()
		_ = p.closeModelStream(stream.StreamID)
		<-readDone
	}()

	holdTimeout := cfg.HoldTimeout.Duration
	if holdTimeout <= 0 {
		holdTimeout = 30 * time.Second
	}
	holdTimer := time.NewTimer(holdTimeout)
	defer holdTimer.Stop()
	holdExpired := false
	emitted := false
	buffer := make([][]byte, 0)
	var bufferBytes int64
	outputNormalizer := &sseOutputNormalizer{}
	appendOutput := func(payloads [][]byte) {
		for _, payload := range payloads {
			if len(payload) == 0 {
				continue
			}
			buffer = append(buffer, payload)
			bufferBytes += int64(len(payload))
		}
	}
	finish := func() transparentStreamAttempt {
		appendOutput(outputNormalizer.Flush())
		if emitted && len(buffer) > 0 {
			if errEmit := p.emitPluginChunks(streamID, buffer); errEmit != nil {
				return transparentStreamAttempt{Class: ClassProbeError, Verdict: QualityVerdict{Class: ClassProbeError, Reason: sanitizeError(errEmit)}, Err: errEmit, Emitted: true}
			}
			buffer = nil
			bufferBytes = 0
		}
		return p.finishTransparentStreamAttempt(
			ctx, execution, callbackID, requestBody, cfg, streamID, analyzer, buffer, emitted, holdExpired,
		)
	}
	reasoningNeeded := reasoningRequiredForModel(execution.Model, requestBody)

	for {
		var timerC <-chan time.Time
		if reasoningNeeded && !emitted && !holdExpired {
			timerC = holdTimer.C
		}
		select {
		case <-ctx.Done():
			return transparentStreamAttempt{Class: ClassProbeError, Verdict: QualityVerdict{Class: ClassProbeError, Reason: sanitizeError(ctx.Err())}, Err: ctx.Err(), Emitted: emitted}
		case <-timerC:
			holdExpired = true
			preview := analyzer.Preview()
			preview.HoldExpired = true
			preview.ReasoningRequired = reasoningNeeded
			verdict := ClassifyQualityHold(preview, cfg.MinHoldOutputTokens)
			if verdict.Action == QualityActionWithhold {
				return transparentStreamAttempt{Class: verdict.Class, Verdict: verdict, Err: fmt.Errorf("quality gate rejected stream: %s", verdict.Class), Buffered: buffer, Available: true}
			}
			if verdict.Action == QualityActionDeliver {
				if errEmit := p.emitPluginChunks(streamID, buffer); errEmit != nil {
					return transparentStreamAttempt{Class: ClassProbeError, Verdict: QualityVerdict{Class: ClassProbeError, Reason: sanitizeError(errEmit)}, Err: errEmit, Available: true}
				}
				emitted = true
				buffer = nil
				bufferBytes = 0
			}
		case event, ok := <-readEvents:
			if !ok {
				return finish()
			}
			if event.Err != nil {
				class := classifyTransportError(event.Err)
				verdict := QualityVerdict{Class: class, Reason: sanitizeError(event.Err)}
				if emitted {
					return transparentStreamAttempt{Class: class, Verdict: verdict, Err: event.Err, Emitted: true}
				}
				return transparentStreamAttempt{Class: class, Verdict: verdict, Err: event.Err}
			}
			if len(event.Response.Payload) > 0 {
				payload := append([]byte(nil), event.Response.Payload...)
				framedPayloads := outputNormalizer.Feed(payload)
				if emitted {
					if errEmit := p.emitPluginChunks(streamID, framedPayloads); errEmit != nil {
						return transparentStreamAttempt{Class: ClassProbeError, Verdict: QualityVerdict{Class: ClassProbeError, Reason: sanitizeError(errEmit)}, Err: errEmit, Emitted: true}
					}
				} else {
					appendOutput(framedPayloads)
					if bufferBytes > cfg.HoldMaxBufferBytes {
						reason := fmt.Sprintf("transparent hold buffer exceeded %d bytes", cfg.HoldMaxBufferBytes)
						return transparentStreamAttempt{Class: ClassProbeError, Verdict: QualityVerdict{Action: QualityActionWithhold, Class: ClassProbeError, Reason: reason}, Err: fmt.Errorf("%s", reason), Buffered: buffer, Available: true}
					}
				}
				analyzer.ObserveChunk(payload)
				if analyzer.Preview().Terminal {
					return finish()
				}
			}
			if event.Response.Error != "" {
				errRead := fmt.Errorf("%s", event.Response.Error)
				class := classifyTransportError(errRead)
				verdict := QualityVerdict{Class: class, Reason: sanitizeError(errRead)}
				if emitted {
					return transparentStreamAttempt{Class: class, Verdict: verdict, Err: errRead, Emitted: true}
				}
				return transparentStreamAttempt{Class: class, Verdict: verdict, Err: errRead}
			}
			if event.Response.Done {
				return finish()
			}
			if emitted {
				continue
			}
			if !reasoningNeeded {
				continue
			}
			preview := analyzer.Preview()
			preview.HoldExpired = holdExpired
			preview.ReasoningRequired = reasoningNeeded
			holdVerdict := ClassifyQualityHold(preview, cfg.MinHoldOutputTokens)
			if holdVerdict.Action == QualityActionWithhold {
				return transparentStreamAttempt{Class: holdVerdict.Class, Verdict: holdVerdict, Err: fmt.Errorf("quality gate rejected stream: %s", holdVerdict.Class), Buffered: buffer, Available: true}
			}
			if holdVerdict.Action == QualityActionDeliver {
				if errEmit := p.emitPluginChunks(streamID, buffer); errEmit != nil {
					return transparentStreamAttempt{Class: ClassProbeError, Verdict: QualityVerdict{Class: ClassProbeError, Reason: sanitizeError(errEmit)}, Err: errEmit, Available: true}
				}
				emitted = true
				buffer = nil
				bufferBytes = 0
				continue
			}
		}
	}
}

func (p *Plugin) finishTransparentStreamAttempt(
	ctx context.Context,
	execution pluginapi.HostModelExecutionRequest,
	callbackID string,
	requestBody []byte,
	cfg QualityConfig,
	streamID string,
	analyzer *StreamAnalyzer,
	buffer [][]byte,
	emitted bool,
	holdExpired bool,
) transparentStreamAttempt {
	sig := analyzer.Finalize(true)
	sig.HoldExpired = holdExpired
	verdict := classifyFinal(sig, cfg, reasoningRequiredForModel(execution.Model, requestBody), cfg.MinHoldOutputTokens, true)
	if emitted {
		_ = p.ApplyVerdict(ctx, execution.AuthID, verdict, "transparent", callbackID)
		return transparentStreamAttempt{Class: verdict.Class, Verdict: verdict, Emitted: true}
	}
	if verdict.Class == ClassNone {
		if errEmit := p.emitPluginChunks(streamID, buffer); errEmit != nil {
			return transparentStreamAttempt{Class: ClassProbeError, Verdict: QualityVerdict{Class: ClassProbeError, Reason: sanitizeError(errEmit)}, Err: errEmit, Available: true}
		}
		_ = p.ApplyVerdict(ctx, execution.AuthID, verdict, "transparent", callbackID)
		return transparentStreamAttempt{Emitted: true}
	}
	return transparentStreamAttempt{Class: verdict.Class, Verdict: verdict, Err: fmt.Errorf("quality gate rejected stream: %s", verdict.Class), Buffered: buffer, Available: true}
}

func (p *Plugin) executableAuth(ctx context.Context, authID string) (Account, map[string]json.RawMessage, bool) {
	account, _, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil || !isGrokAuth(raw) || authDisabled(raw) {
		return Account{}, nil, false
	}
	if errValidate := validateAuthPayload(raw); errValidate != nil {
		return Account{}, nil, false
	}
	return account, raw, true
}

func transparentMaxAttempts(policy Policy, replayable bool) int {
	if !replayable {
		return 1
	}
	maxAttempts := policy.Quality.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 6
	}
	return maxAttempts
}

func shouldRotateTransparentFailure(candidateCount int, class string) bool {
	// Missing thinking is account-level evidence. Switch accounts first when an
	// alternative exists; keep proxy rotation as the single-account fallback.
	return class != ClassMissingThinking || candidateCount <= 1
}

func executorRequestInput(req rpcExecutorRequest) ([]byte, string, error) {
	if len(req.Payload) > 0 {
		sourceFormat := firstNonEmpty(req.SourceFormat, req.Format, "codex")
		return req.Payload, sourceFormat, nil
	}
	body := req.OriginalRequest
	if len(body) == 0 {
		return nil, "", fmt.Errorf("request body is empty")
	}
	sourceFormat := firstNonEmpty(req.SourceFormat, req.Format, "codex")
	return body, sourceFormat, nil
}

func executionModel(req rpcExecutorRequest, body []byte) string {
	return firstNonEmpty(req.Model, asString(req.Metadata["model"]), modelFromRequestBody(body), req.Alt)
}

// ensureTransparentReasoningEvidence mirrors the grok2api Build/Console request
// boundary. Codex and Responses clients may omit encrypted reasoning from the
// include list, which makes a healthy stream indistinguishable from a degraded
// one. Add the include before the quality gate observes the upstream stream.
func ensureTransparentReasoningEvidence(body []byte, sourceFormat, model string) []byte {
	if len(body) == 0 {
		return body
	}
	format := strings.ToLower(strings.TrimSpace(sourceFormat))
	if !strings.Contains(format, "codex") && !strings.Contains(format, "response") {
		return body
	}
	if !reasoningRequiredForModel(model, body) {
		return body
	}
	var payload map[string]json.RawMessage
	if errUnmarshal := json.Unmarshal(body, &payload); errUnmarshal != nil || payload == nil {
		return body
	}
	if _, ok := payload["input"]; !ok {
		return body
	}
	includes := make([]string, 0)
	if raw, exists := payload["include"]; exists && len(raw) > 0 && string(raw) != "null" {
		if errUnmarshal := json.Unmarshal(raw, &includes); errUnmarshal != nil {
			return body
		}
		for _, name := range includes {
			if strings.TrimSpace(name) == "" {
				continue
			}
			if name == "reasoning.encrypted_content" {
				return body
			}
		}
	}
	encodedInclude, errMarshalInclude := json.Marshal(append(includes, "reasoning.encrypted_content"))
	if errMarshalInclude != nil {
		return body
	}
	payload["include"] = encodedInclude
	encoded, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return body
	}
	return encoded
}

func buildHostExecution(req rpcExecutorRequest, body []byte, sourceFormat, authID, proxyURL string, stream bool) pluginapi.HostModelExecutionRequest {
	model := executionModel(req, body)
	return pluginapi.HostModelExecutionRequest{
		EntryProtocol:  sourceFormat,
		ExitProtocol:   firstNonEmpty(req.Format, sourceFormat),
		Model:          model,
		Stream:         stream,
		Body:           body,
		Headers:        cloneHeader(req.Headers),
		Query:          cloneValues(req.Query),
		Alt:            req.Alt,
		ForcedProvider: "xai",
		AuthID:         authID,
		ProxyURL:       strings.TrimSpace(proxyURL),
	}
}

func (p *Plugin) emitPluginChunks(streamID string, payloads [][]byte) error {
	for _, payload := range payloads {
		if errEmit := p.emitPluginStreamChunk(streamID, payload); errEmit != nil {
			return errEmit
		}
	}
	return nil
}

func (p *Plugin) emitPluginStreamChunk(streamID string, payload []byte) error {
	if strings.TrimSpace(streamID) == "" {
		return fmt.Errorf("plugin stream id is required")
	}
	_, errCall := hostCall(pluginabi.MethodHostStreamEmit, rpcStreamEmitRequest{
		StreamID: streamID,
		Payload:  append([]byte(nil), payload...),
	})
	return errCall
}

func (p *Plugin) closePluginStream(streamID, code, message string, status int) {
	if strings.TrimSpace(streamID) == "" {
		return
	}
	request := rpcStreamCloseRequest{StreamID: streamID}
	if strings.TrimSpace(message) != "" || status != 0 {
		request.ErrorDetail = &pluginabi.Error{
			Code:       strings.TrimSpace(code),
			Message:    strings.TrimSpace(message),
			HTTPStatus: status,
		}
	}
	_, _ = hostCall(pluginabi.MethodHostStreamClose, request)
}

func isTransparentCandidateState(state string) bool {
	switch state {
	case "", StateHealthy, StateUnknown:
		return true
	default:
		return false
	}
}

func (p *Plugin) recordAttemptError(authID, class string, errValue error) error {
	if p.stateStore() == nil {
		return errValue
	}
	return p.stateStore().Update(func(state *State) error {
		p.recordEvent(state, authID, class, "transparent_attempt", "failed", sanitizeError(errValue))
		return nil
	})
}

func (p *Plugin) applyTransparentFailureVerdict(ctx context.Context, authID string, verdict QualityVerdict, class string, errValue error, callbackID string) {
	if verdict.Class == "" {
		verdict.Class = class
	}
	if verdict.Class == "" || verdict.Class == ClassIgnored {
		return
	}
	if verdict.Reason == "" && errValue != nil {
		verdict.Reason = sanitizeError(errValue)
	}
	_ = p.ApplyVerdict(ctx, authID, verdict, "transparent", callbackID)
}

// rotateAfterFailure runs before the verdict is applied so the same auth can retry on the new proxy
func (p *Plugin) rotateAfterFailure(ctx context.Context, authID, class string) (bool, error) {
	resinConfig := p.ResinConfig()
	if !resinConfig.Enabled || !resinConfig.AutoRotateAfterQualityFailure {
		return false, nil
	}
	if class != ClassMissingThinking && class != ClassCipherDrool && class != ClassFakeEncryptedDump && class != ClassBufferedBurst {
		return false, nil
	}
	stateAccount, _ := p.stateAccount(authID)
	if isIsolatedState(stateAccount.State) {
		return false, nil
	}
	_, _, raw, errResolve := p.resolveAuth(ctx, authID)
	if errResolve != nil {
		_ = p.recordAttemptError(authID, ClassProxyError, errResolve)
		return false, errResolve
	}
	if _, managed := managedResinBinding(raw, stateAccount, resinConfig); !managed {
		return false, nil
	}
	if errRotate := p.RotateResin(ctx, authID); errRotate != nil {
		_ = p.recordAttemptError(authID, ClassProxyError, errRotate)
		return false, errRotate
	}
	return true, nil
}

const maxUpstreamErrorBodyBytes = 64 << 10

func classifyProbeStatus(status int) string {
	class, _ := classifyUpstreamStatus(status, nil)
	return class
}

func classifyUpstreamStatus(status int, body []byte) (string, string) {
	switch status {
	case http.StatusUnauthorized:
		return ClassCredentialInvalid, "upstream status 401 (credential rejected)"
	case http.StatusProxyAuthRequired:
		return ClassProxyError, "upstream status 407 (proxy authentication required)"
	case http.StatusPaymentRequired:
		return ClassQuotaExhausted, "upstream status 402 (quota exhausted)"
	}
	text := strings.ToLower(string(body))
	if status == http.StatusForbidden || status == http.StatusTooManyRequests {
		if isQuotaExhaustionText(text) {
			return ClassQuotaExhausted, fmt.Sprintf("upstream status %d (quota exhausted)", status)
		}
		if status == http.StatusForbidden && isCredentialRejectionText(text) {
			return ClassCredentialInvalid, "upstream status 403 (credential rejected)"
		}
	}
	return ClassIgnored, fmt.Sprintf("upstream status %d", status)
}

func isQuotaExhaustionText(text string) bool {
	for _, marker := range []string{
		"subscription:free-usage-exhausted",
		"used all the included free usage",
		"usage limit reached",
		"usage balance exhausted",
		"run out of credits",
		"out of credits",
		"spending-limit",
		"quota",
		"tokens (actual/limit)",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func isCredentialRejectionText(text string) bool {
	for _, marker := range []string{"authentication", "unauthorized", "invalid token", "token expired"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func (p *Plugin) readModelStreamErrorBody(ctx context.Context, streamID string) []byte {
	if p == nil || strings.TrimSpace(streamID) == "" {
		return nil
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	body := make([]byte, 0, 4096)
	for len(body) < maxUpstreamErrorBodyBytes {
		if readCtx.Err() != nil {
			break
		}
		read, errRead := p.readModelStreamContext(readCtx, streamID)
		if errRead != nil {
			break
		}
		if len(read.Payload) > 0 {
			remaining := maxUpstreamErrorBodyBytes - len(body)
			if len(read.Payload) > remaining {
				read.Payload = read.Payload[:remaining]
			}
			body = append(body, read.Payload...)
		}
		if read.Error != "" {
			remaining := maxUpstreamErrorBodyBytes - len(body)
			if remaining > 0 {
				errorText := []byte(read.Error)
				if len(errorText) > remaining {
					errorText = errorText[:remaining]
				}
				body = append(body, errorText...)
			}
		}
		if read.Done {
			break
		}
	}
	return body
}

func classifyTransportFailure(errValue error) (string, string) {
	if errValue == nil {
		return ClassProbeError, "probe transport error"
	}
	text := strings.ToLower(errValue.Error())
	if strings.Contains(text, "proxy") || strings.Contains(text, "resin") || strings.Contains(text, "socks") {
		return ClassProxyError, "probe transport error"
	}
	if isQuotaExhaustionText(text) {
		return ClassQuotaExhausted, "host_call_failed: quota exhausted"
	}
	for _, marker := range []string{"invalid token", "expired", "no auth", "quota", "rate limit", "ratelimit", "too many requests", "permission denied", "forbidden", "unauthorized"} {
		if strings.Contains(text, marker) {
			return ClassIgnored, "probe transport error"
		}
	}
	return ClassProbeError, "probe transport error"
}

func classifyTransportError(errValue error) string {
	class, _ := classifyTransportFailure(errValue)
	return class
}

func sseHeaders() http.Header {
	return http.Header{
		"Content-Type":  []string{"text/event-stream"},
		"Cache-Control": []string{"no-cache"},
	}
}

func safeReplayRequest(body []byte, sourceFormat, _ string) bool {
	if len(body) == 0 {
		return false
	}
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(body, &payload); errUnmarshal != nil {
		return false
	}
	format := strings.ToLower(strings.TrimSpace(sourceFormat))
	if strings.Contains(format, "image") || strings.Contains(format, "video") || strings.Contains(format, "audio") || strings.Contains(format, "speech") || strings.Contains(format, "realtime") || strings.Contains(format, "websocket") {
		return false
	}
	if nonEmptyJSONValue(payload["previous_response_id"]) || nonEmptyJSONValue(payload["conversation"]) {
		return false
	}
	for _, key := range []string{"store", "background"} {
		if value, ok := payload[key].(bool); ok && value {
			return false
		}
	}
	if containsCompactionTrigger(payload) || containsUnsafeContentType(payload) {
		return false
	}
	if hasNonTextModality(payload["modalities"]) {
		return false
	}
	if value, ok := payload["web_search_options"]; ok && value != nil {
		return false
	}
	if nonEmptyJSONValue(payload["mcp_servers"]) {
		return false
	}
	if rawTools, okTools := payload["tools"]; okTools {
		tools, ok := rawTools.([]any)
		if !ok {
			return false
		}
		for _, rawTool := range tools {
			tool, _ := rawTool.(map[string]any)
			if tool == nil {
				continue
			}
			kind := strings.ToLower(asString(tool["type"]))
			switch kind {
			case "function", "custom", "local_shell", "apply_patch", "tool_search", "shell":
				if kind == "shell" && !localShellTool(tool) {
					return false
				}
			default:
				return false
			}
		}
	}
	if reasoningDisabledBody(payload) {
		return false
	}
	return true
}

func nonEmptyJSONValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

func containsCompactionTrigger(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if strings.EqualFold(strings.TrimSpace(key), "compaction_trigger") {
				return true
			}
			if containsCompactionTrigger(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsCompactionTrigger(child) {
				return true
			}
		}
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "compaction_trigger")
	}
	return false
}

const clientCompactionPromptMarker = "it is a system-generated compaction prompt, not a real user message"

func isResponsesCompactionRequest(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(body, &payload); errUnmarshal != nil || payload == nil {
		return false
	}
	if hasCompactionTriggerItem(payload["input"]) {
		return true
	}
	return lastItemLooksLikeCompactionPrompt(payload["input"]) || lastItemLooksLikeCompactionPrompt(payload["messages"])
}

func hasCompactionTriggerItem(value any) bool {
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, rawItem := range items {
		item, _ := rawItem.(map[string]any)
		if item != nil && strings.EqualFold(strings.TrimSpace(asString(item["type"])), "compaction_trigger") {
			return true
		}
	}
	return false
}

func lastItemLooksLikeCompactionPrompt(value any) bool {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return false
	}
	item, _ := items[len(items)-1].(map[string]any)
	if item == nil || !strings.EqualFold(strings.TrimSpace(asString(item["role"])), "user") {
		return false
	}
	return strings.Contains(strings.ToLower(compactContentText(item["content"])), clientCompactionPromptMarker)
}

func compactContentText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		var text strings.Builder
		for _, rawPart := range typed {
			part, _ := rawPart.(map[string]any)
			if part == nil {
				continue
			}
			text.WriteString(asString(part["text"]))
		}
		return text.String()
	default:
		return ""
	}
}

func localShellTool(tool map[string]any) bool {
	env, _ := tool["environment"].(map[string]any)
	if env == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(asString(env["type"])), "local")
}

func reasoningDisabledBody(payload map[string]any) bool {
	if effort := strings.ToLower(strings.TrimSpace(asString(payload["reasoning_effort"]))); effort == "none" || effort == "off" {
		return true
	}
	for _, key := range []string{"reasoning", "output_config", "thinking"} {
		if value, ok := payload[key]; ok && disabledReasoningValue(value) {
			return true
		}
	}
	return false
}

func containsUnsafeContentType(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		if kind := strings.ToLower(strings.TrimSpace(asString(typed["type"]))); isUnsafeContentType(kind) {
			return true
		}
		for _, child := range typed {
			if containsUnsafeContentType(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsUnsafeContentType(child) {
				return true
			}
		}
	}
	return false
}

func isUnsafeContentType(kind string) bool {
	switch kind {
	case "image", "image_url", "input_image", "audio", "input_audio", "file", "input_file", "video", "input_video", "speech", "realtime", "computer_screenshot":
		return true
	default:
		return false
	}
}

func hasNonTextModality(value any) bool {
	values, ok := value.([]any)
	if !ok {
		return false
	}
	for _, raw := range values {
		switch strings.ToLower(strings.TrimSpace(asString(raw))) {
		case "", "text":
			continue
		default:
			return true
		}
	}
	return false
}
