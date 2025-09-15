from ray.serve.llm import LLMConfig, LLMRouter, LLMServer
from ray.serve.config import RequestRouterConfig
from ray.serve.context import _get_internal_replica_context
from ray.serve.deployment import Application
from ray.serve._private.common import ReplicaID

from .session_router import SessionAwareRequestRouter
from ray import serve

from typing import Dict, Any

class SesssionAwareMixin:
    
    async def __init__(self):
        
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id
        
        print(f"[DEBUG] Inside the constructor of SesssionAwareMixin on replica {self.replica_id.unique_id}")
        
        # TODO: Make this a Capped set of size 1000 or sth that has LRU eviction
        self.hot_sessions = set()
        
    def _parse_session_id(self, request):
        # Extra args from request
        xargs = getattr(request, "vllm_xargs", {})
        return xargs.get("session_id")
    
    def record_routing_stats(self) -> Dict[str, Any]:
        print(f"[DEBUG] Reporting hot_sessions: {self.hot_sessions}")
        return {
            "hot_sessions": self.hot_sessions
        }

class SessionAwareLLMServer(SesssionAwareMixin, LLMServer):
    async def __init__(self):
        a = 1 / 0
        await LLMServer.__init__(self)
        await SesssionAwareMixin.__init__(self)
        print(f"[DEBUG] Inside the constructor of SessionAwareLLMServer ...")
        
    async def _run_request(
        self,
        request,
        *,
        engine_method: str,
        batch_output_stream: bool = False,
    ):
        # from session aware mixin
        session_id = self._parse_session_id(request)
        print(f"[DEBUG] Session_id={session_id} hit replica {self.replica_id.unique_id}")
        if session_id:
            self.hot_sessions.add(session_id)
        
        return await super()._run_request(
            request,
            engine_method=engine_method,
            batch_output_stream=batch_output_stream,
        )

def build(serving_config_dict: Dict[str, Any]) -> Application:
    
    llm_configs = serving_config_dict["llm_configs"]
    
    if len(llm_configs) != 1:
        raise ValueError("Only one LLM config is supported")
    
    
    llm_config = LLMConfig(**llm_configs[0])
    # TODO: Builder of LLM Deployment is not generic enough for custom LLMServers. 
    # llm_deployment = build_llm_deployment(llm_config)
    deployment_options = llm_config.get_serve_options(
        name_prefix="LLMServer:",
    )
    
    # Update the deployment option with the session aware request router
    deployment_options["request_router_config"] = RequestRouterConfig(
        request_router_class=SessionAwareRequestRouter,
        request_routing_stats_period_s=2,
        request_routing_stats_timeout_s=1,
    )

    llm_deployment = serve.deployment(SessionAwareLLMServer).options(**deployment_options).bind(llm_config=llm_config)
    app = LLMRouter.as_deployment(llm_configs=[llm_config]).bind(
        llm_deployments=[llm_deployment])
    return app