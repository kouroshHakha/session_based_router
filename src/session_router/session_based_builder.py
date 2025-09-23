from ray.serve.llm import LLMConfig, LLMRouter, LLMServer
from ray.serve.config import RequestRouterConfig
from ray.serve.context import _get_internal_replica_context
from ray.serve.deployment import Application
from ray.serve._private.common import ReplicaID

from .session_router import SessionAwareRequestRouter
from .http_header_llm_router import HttpHeaderLLMRouter
from ray import serve

from typing import Dict, Any

import logging
logger = logging.getLogger(__name__)


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
    )

    llm_deployment = serve.deployment(LLMServer).options(**deployment_options).bind(llm_config=llm_config)
    app = HttpHeaderLLMRouter.as_deployment(llm_configs=[llm_config]).bind(
        llm_deployments=[llm_deployment])
    return app