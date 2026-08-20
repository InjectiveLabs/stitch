{{/*
The base resource name intentionally does not include Release.Name. This keeps
the default names short and predictable; fullnameOverride is required for a
second installation in the same namespace.
*/}}
{{- define "stitch.fullname" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stitch.configName" -}}
{{- if .Values.config.existingConfigMap -}}
{{- .Values.config.existingConfigMap -}}
{{- else if .Values.config.existingSecret -}}
{{- .Values.config.existingSecret -}}
{{- else -}}
{{- printf "%s-config" (include "stitch.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "stitch.serviceName" -}}
{{- printf "%s-service" (include "stitch.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stitch.routeName" -}}
{{- printf "%s-%s" (include "stitch.fullname" .root) .name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Build the Kubernetes listener map from Stitch's structured listen values.
An explicit listeners map is an escape hatch for raw or externally managed
configuration. Underscores in Stitch listener names become Kubernetes-safe
hyphens.
*/}}
{{- define "stitch.effectiveListeners" -}}
{{- if gt (len .Values.listeners) 0 -}}
{{- toYaml .Values.listeners -}}
{{- else -}}
{{- $listenConfig := .Values.listen -}}
{{- if .Values.config.content -}}
  {{- $rawConfig := fromYaml .Values.config.content -}}
  {{- if hasKey $rawConfig "Error" -}}
    {{- fail (printf "config.content is not valid YAML: %s" (index $rawConfig "Error")) -}}
  {{- end -}}
  {{- $listenConfig = default (dict) (index $rawConfig "listen") -}}
{{- end -}}
{{- $result := dict -}}
{{- $protocols := dict
  "rpc" "http"
  "grpc" "kubernetes.io/h2c"
  "api" "http"
  "eth_rpc" "http"
  "eth_ws" "kubernetes.io/ws"
  "chainstream" "kubernetes.io/h2c"
  "inj_ws" "kubernetes.io/ws"
  "admin" "http"
-}}
{{- range $configName, $appProtocol := $protocols -}}
  {{- $listener := index $listenConfig $configName -}}
  {{- if and $listener (index $listener "addr") -}}
    {{- $addr := toString (index $listener "addr") -}}
    {{- if not (regexMatch ":[0-9]+$" $addr) -}}
      {{- fail (printf "listen.%s.addr=%q must end with :<port>" $configName $addr) -}}
    {{- end -}}
    {{- $portText := regexFind "[0-9]+$" $addr -}}
    {{- $port := atoi $portText -}}
    {{- if or (lt $port 1) (gt $port 65535) -}}
      {{- fail (printf "listen.%s.addr=%q contains an invalid port" $configName $addr) -}}
    {{- end -}}
    {{- $serviceName := replace "_" "-" $configName -}}
    {{- $_ := set $result $serviceName (dict "enabled" true "port" $port "appProtocol" $appProtocol) -}}
  {{- end -}}
{{- end -}}
{{- toYaml $result -}}
{{- end -}}
{{- end -}}

{{- define "stitch.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stitch.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stitch.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "stitch.labels" -}}
helm.sh/chart: {{ include "stitch.chart" . }}
{{ include "stitch.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}
