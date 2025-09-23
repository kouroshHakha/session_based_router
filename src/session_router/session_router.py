
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.pow_2_router import PowerOfTwoChoicesRequestRouter
from ray.serve._private.common import ReplicaID
from ray.serve.handle import ReplicaResult

from typing import List, Optional

import logging
import hashlib
logger = logging.getLogger(__name__)

class SessionAwareRequestRouter(PowerOfTwoChoicesRequestRouter):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"[DEBUG] SessionAwareRequestRouter initialized.")


    def _extract_session_id(self, request: PendingRequest):
        for idx, arg in enumerate(request.args):
            xargs = None
            if isinstance(arg, dict):
                xargs = arg.get("vllm_xargs", {})
                logger.info(f"_extract_session_id: arg[{idx}] is dict, vllm_xargs={xargs}")
            else:
                xargs = getattr(arg, "vllm_xargs", {})
                logger.info(f"_extract_session_id: arg[{idx}] is {type(arg)}, vllm_xargs={xargs}")
            if xargs: 
                session_id = xargs.get("session_id")
                logger.info(f"_extract_session_id: Found session_id={session_id}")
                return session_id

        logger.info("_extract_session_id: No session_id found in request.args")
        return None
        

    def _find_matched_replica(self, candidate_replicas: List[RunningReplica], pending_request: Optional[PendingRequest]) -> Optional[RunningReplica]:
        """
        Find the replica that matches the session-id in the request.
        """
        if not pending_request:
            return 
        
        session_id = self._extract_session_id(pending_request)
        logger.info(f"[DEBUG] Session-aware routing: session_id={session_id}")
        if session_id:
            candidate_replicas.sort(key=lambda x: x.replica_id.to_full_id_str())

            index = int(hashlib.md5(session_id.encode()).hexdigest(), 16)
            return candidate_replicas[index % len(candidate_replicas)]
            
        return None
    
    
    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:

        # Update our session mapping from latest replica stats
        matched_replica = self._find_matched_replica(candidate_replicas, pending_request)
        
        if matched_replica:
            logger.info(f"Using session-aware routing: replica_id={matched_replica.replica_id}")
            return [[matched_replica]]

        # Fallback to PowerOfTwoChoicesRequestRouter
        logger.info(f"Using fallback routing: candidate_replicas={candidate_replicas}")
        return await super().choose_replicas(candidate_replicas, pending_request)
    