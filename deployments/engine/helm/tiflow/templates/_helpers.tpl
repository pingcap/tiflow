{{- define "tiflow.name" -}}
{{- if .Values.clusterName -}}
{{ .Values.clusterName }}-{{ .Release.Name }}
{{- else -}}
{{ .Release.Name }}
{{- end -}}
{{- end -}}

{{- define "tiflow.masterAddr" -}}
{{- $replicas := int .Values.master.replicas -}}
{{- if eq $replicas 1 -}}
{{ include "tiflow.name" . }}-server-master-0.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10240
{{- else if eq $replicas 2 -}}
{{ include "tiflow.name" . }}-server-master-0.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10240,{{ include "tiflow.name" . }}-server-master-1.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10240
{{- else if eq $replicas 3 -}}
{{ include "tiflow.name" . }}-server-master-0.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10240,{{ include "tiflow.name" . }}-server-master-1.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10240,{{ include "tiflow.name" . }}-server-master-2.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10240
{{- end -}}
{{- end -}}

{{- define "tiflow.configmap.data" -}}
server-master: |-
    framework-meta.endpoints = ["{{ include "tiflow.name" . }}-metastore-mysql:3306"]
    business-meta.endpoints = ["{{ include "tiflow.name" . }}-metastore-mysql:3306"]
    {{- if .Values.master.config }}
{{ .Values.master.config | indent 4 }}
    {{- end }}
executor: |-
    {{- if .Values.executor.config }}
{{ .Values.executor.config | indent 4 }}
    {{- end }}
{{- end -}}
