{{/*
Render-time checks that are awkward to express in JSON Schema.
*/}}
{{- define "stitch.validate" -}}
{{- $listeners := include "stitch.effectiveListeners" . | fromYaml -}}
{{- $managedConfig := and (not .Values.config.existingConfigMap) (not .Values.config.existingSecret) -}}
{{- $structuredConfig := and $managedConfig (not .Values.config.content) -}}
{{- if and $structuredConfig (eq (len .Values.backends) 0) -}}
  {{- fail "backends must contain at least one entry when using structured Stitch configuration values" -}}
{{- end -}}
{{- if and $structuredConfig (gt (len .Values.listeners) 0) -}}
  {{- fail "listeners overrides cannot be used with structured configuration; listener ports are derived from the top-level listen values" -}}
{{- end -}}

{{- $enabledListeners := 0 -}}
{{- $ports := dict -}}
{{- range $name, $listener := $listeners -}}
  {{- if $listener.enabled -}}
    {{- $enabledListeners = add1 $enabledListeners -}}
    {{- $portKey := printf "%d" (int $listener.port) -}}
    {{- if hasKey $ports $portKey -}}
      {{- fail (printf "listeners %q and %q both use port %d" (index $ports $portKey) $name (int $listener.port)) -}}
    {{- end -}}
    {{- $_ := set $ports $portKey $name -}}
  {{- end -}}
{{- end -}}
{{- if eq (int $enabledListeners) 0 -}}
  {{- fail "at least one listener must be enabled" -}}
{{- end -}}

{{- $probesEnabled := or .Values.probes.startup.enabled .Values.probes.readiness.enabled .Values.probes.liveness.enabled -}}
{{- $adminListener := index $listeners "admin" -}}
{{- if and $probesEnabled (or (not $adminListener) (not $adminListener.enabled)) -}}
  {{- fail "listeners.admin.enabled must be true while any HTTP probe is enabled" -}}
{{- end -}}
{{- if and $managedConfig $probesEnabled $adminListener -}}
  {{- $listenConfig := .Values.listen -}}
  {{- if .Values.config.content -}}
    {{- $rawConfig := fromYaml .Values.config.content -}}
    {{- $listenConfig = default (dict) (index $rawConfig "listen") -}}
  {{- end -}}
  {{- $adminConfig := index $listenConfig "admin" -}}
  {{- if $adminConfig -}}
    {{- $adminAddr := toString (index $adminConfig "addr") -}}
    {{- if regexMatch "^(127\\.0\\.0\\.1|localhost|\\[?::1\\]?):" $adminAddr -}}
      {{- fail (printf "listen.admin.addr=%q is loopback-only; bind admin to 0.0.0.0 while Kubernetes probes are enabled, or disable all probes" $adminAddr) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{- if .Values.gateway.enabled -}}
  {{- if and (eq (len .Values.gateway.httpRoutes) 0) (eq (len .Values.gateway.grpcRoutes) 0) -}}
    {{- fail "gateway.enabled is true but no HTTP or gRPC routes are configured" -}}
  {{- end -}}
  {{- $routeNames := dict -}}
  {{- range $route := concat .Values.gateway.httpRoutes .Values.gateway.grpcRoutes -}}
    {{- if hasKey $routeNames $route.name -}}
      {{- fail (printf "gateway route name %q is duplicated" $route.name) -}}
    {{- end -}}
    {{- $_ := set $routeNames $route.name true -}}
    {{- $listener := index $listeners $route.targetPort -}}
    {{- if not $listener -}}
      {{- fail (printf "gateway route %q targets listener %q, which is not configured" $route.name $route.targetPort) -}}
    {{- end -}}
    {{- if not $listener.enabled -}}
      {{- fail (printf "gateway route %q targets disabled listener %q" $route.name $route.targetPort) -}}
    {{- end -}}
    {{- $parentRefs := default $.Values.gateway.parentRefs $route.parentRefs -}}
    {{- if eq (len $parentRefs) 0 -}}
      {{- fail (printf "gateway route %q needs parentRefs, either on the route or at gateway.parentRefs" $route.name) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- end -}}
