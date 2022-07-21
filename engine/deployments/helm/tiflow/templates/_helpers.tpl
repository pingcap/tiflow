{{- define "tiflow.clusterType" -}}
{{ .Values.master.replicas }}m{{ .Values.executor.replicas }}e
{{- end -}}

{{- define "tiflow.name" -}}
{{- if .Values.clusterName -}}
{{ .Values.clusterName }}-{{ .Release.Name }}-{{ include "tiflow.clusterType" . }}
{{- else -}}
{{ .Release.Name }}-{{ include "tiflow.clusterType" . }}
{{- end -}}
{{- end -}}

{{- define "tiflow.initialCluster" -}}
{{- $replicas := int .Values.master.replicas -}}
{{- if eq $replicas 1 -}}
{{ include "tiflow.name" . }}-server-master-0=http://{{ include "tiflow.name" . }}-server-master-0.$(MY_SERVICE_NAME).$(MY_POD_NAMESPACE):10239
{{- else if eq $replicas 2 -}}
{{ include "tiflow.name" . }}-server-master-0=http://{{ include "tiflow.name" . }}-server-master-0.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10239,{{ include "tiflow.name" . }}-server-master-1=http://{{ include "tiflow.name" . }}-server-master-1.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10239
{{- else if eq $replicas 3 -}}
{{ include "tiflow.name" . }}-server-master-0=http://{{ include "tiflow.name" . }}-server-master-0.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10239,{{ include "tiflow.name" . }}-server-master-1=http://{{ include "tiflow.name" . }}-server-master-1.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10239,{{ include "tiflow.name" . }}-server-master-2=http://{{ include "tiflow.name" . }}-server-master-2.{{ include "tiflow.name" . }}-server-master.$(MY_POD_NAMESPACE):10239
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


