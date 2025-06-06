import { FlowView } from '@/views/flow/flow-view'
import { FlowConfigResponse, FlowResponse } from '@/views/flow/hooks/use-get-flow-state'
import { useCallback, useEffect, useState } from 'react'
import { useParams } from 'react-router'

export const Flow = () => {
  const { id } = useParams()
  const flowId = id!
  const [flow, setFlow] = useState<FlowResponse | null>(null)
  const [flowConfig, setFlowConfig] = useState<FlowConfigResponse | null>(null)

  const fetchFlow = useCallback(() => {
    Promise.all([fetch(`/flows/${flowId}`), fetch(`/flows/${flowId}/config`)])
      .then(([flowRes, configRes]) => Promise.all([flowRes.json(), configRes.json()]))
      .then(([flow, config]) => {
        setFlow(flow)
        setFlowConfig(config)
      })
  }, [flowId])

  useEffect(fetchFlow, [fetchFlow])

  if (!flow || flow.error)
    return (
      <div className="w-full h-full bg-background flex flex-col items-center justify-center">
        <p>{flow?.error}</p>
      </div>
    )

  return (
    <div className="w-full h-full bg-background">
      <FlowView flow={flow} flowConfig={flowConfig!} />
    </div>
  )
}
