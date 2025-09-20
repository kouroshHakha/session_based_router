
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.pow_2_router import PowerOfTwoChoicesRequestRouter
from ray.serve._private.common import ReplicaID
from ray.serve.handle import ReplicaResult

from typing import List, Optional
from .session_store import get_request_session_mapping, delete_request_session_mapping, get_replica_for_session, associate_session_with_replica

import logging
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
            replica = candidate_replicas[hash(session_id) % len(candidate_replicas)]
            logger.info(f"[DEBUG] Session-aware routing: replica_id={replica.replica_id}")
            return replica
            
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
    
    # def on_request_routed(
    #     self,
    #     pending_request: PendingRequest,
    #     replica_id: ReplicaID,
    #     result: ReplicaResult,
    # ):
    #     """Called when a request is routed to a replica.
        
    #     Associates the session ID with the replica ID if they don't already have a mapping.
    #     """
    #     session_id = self._extract_session_id(pending_request)
    #     replica_id_str = replica_id.to_full_id_str()
    #     if session_id:
    #         # Check if we already have a mapping for this session
    #         existing_replica = get_replica_for_session(session_id)
    #         if not existing_replica:
    #             # Associate this session with the replica it was routed to
    #             logger.info(f"Associating session {session_id} with replica {replica_id_str}")
    #             associate_session_with_replica(session_id, replica_id_str)
    #         else:
    #             logger.info(f"Session {session_id} already associated with replica {existing_replica}")
        
    #     # Call parent implementation
    #     super().on_request_routed(pending_request, replica_id, result)
