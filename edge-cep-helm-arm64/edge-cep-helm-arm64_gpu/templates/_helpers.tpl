{{- define "edgecep.name" -}}
edge-cep-demo
{{- end -}}

{{- define "edgecep.labels" -}}
app.kubernetes.io/name: {{ include "edgecep.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}