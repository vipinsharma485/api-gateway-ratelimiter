# ADR-0003: Distributed tracing via Micrometer Tracing + OpenTelemetry → Jaeger

**Status:** Accepted
**Date:** 2026-06-19
**Decision driver:** Vipin Sharma

## Context

A request now crosses at least two services (gateway → product-service) and several infrastructure hops (Redis for rate limiting, the circuit breaker, Kafka for audit events). When something is slow or fails, logs alone don't show *where* in that chain the time went or the error originated. We need request-scoped, cross-service traces with a single trace ID that ties the spans together.

Requirements:
- One trace spanning the gateway and every downstream it calls, correlated by a shared trace ID
- Minimal, vendor-neutral instrumentation — not tied to one tracing backend
- Works for both the reactive gateway (WebFlux) and the MVC product-service
- A local backend to visualise traces during development

## Decision

Instrument both services with **Micrometer Tracing**, bridge it to **OpenTelemetry**, and export spans over **OTLP** to **Jaeger**.

- `spring-boot-starter-actuator` brings the observation/tracing auto-configuration that instruments incoming HTTP requests and outgoing clients automatically
- `spring-boot-starter-opentelemetry` (new in Spring Boot 4) bundles the OpenTelemetry API, the Micrometer→OTel tracing bridge, and the OTLP exporter in a single starter — replacing the separate `micrometer-tracing-bridge-otel` + `opentelemetry-exporter-otlp` dependencies used in Boot 3
- Spring Boot auto-configures an `OtlpHttpSpanExporter` pointed at `management.opentelemetry.tracing.export.otlp.endpoint` (the Boot 4 property; Boot 3 used `management.otlp.tracing.endpoint`)
- Jaeger all-in-one runs in `docker-compose` with its OTLP receiver enabled; the endpoint is wired per-service via the `OTLP_ENDPOINT` env var (`http://jaeger:4318/v1/traces` in Compose, localhost otherwise)
- Each service sets `spring.application.name`, which becomes the service name on its spans
- Trace context propagates across the gateway → product-service hop automatically via the standard W3C `traceparent` header, so both services' spans share one trace ID
- Sampling is `1.0` (100%) for development; this is the first knob to lower in production

## Considered alternatives

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **No tracing** | Nothing to run | Can't see cross-service latency or where failures originate | Rejected |
| **Micrometer + OTLP → Jaeger (chosen)** | Vendor-neutral API; auto-instrumentation; OTLP is the industry standard; Jaeger speaks OTLP natively | Adds deps + a backend to run | **Accepted** |
| **Micrometer + Brave/Zipkin bridge** | Mature, lightweight | Zipkin wire format is the older path; OTLP is where the ecosystem is heading | Rejected — prefer OTLP |
| **OpenTelemetry Java agent (bytecode)** | Zero code, broad auto-instrumentation | Opaque, heavier, another moving part in the image; less control | Rejected for now — revisit at scale |

## Consequences

### Positive
- One trace ID follows a request across the gateway and product-service; spans show exactly where latency or errors occur
- Vendor-neutral: swapping Jaeger for Tempo, Honeycomb, or any OTLP backend is a config change, not a code change
- Trace IDs can be added to logs (MDC) for log↔trace correlation, and the same observation registry feeds Phase 7 metrics

### Negative
- Two more runtime dependencies per service and a Jaeger container to run locally
- 100% sampling is fine for dev but would be too much data/overhead in production — must be lowered (head or tail sampling)
- The OTLP exporter logs warnings if no collector is reachable; harmless, but noisy when running a service without Jaeger up

## References

- Spring Boot reference: [Tracing](https://docs.spring.io/spring-boot/reference/actuator/tracing.html)
- Micrometer Tracing docs: https://docs.micrometer.io/tracing/reference/
- OpenTelemetry OTLP spec: https://opentelemetry.io/docs/specs/otlp/
- Jaeger: [OpenTelemetry / OTLP ingestion](https://www.jaegertracing.io/docs/latest/apis/#opentelemetry-protocol)
