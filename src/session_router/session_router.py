
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.pow_2_router import PowerOfTwoChoicesRequestRouter

from typing import List, Optional
from .session_store import get_request_session_mapping, delete_request_session_mapping

import logging
logger = logging.getLogger(__name__)

class SessionAwareRequestRouter(PowerOfTwoChoicesRequestRouter):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"[DEBUG] SessionAwareRequestRouter initialized.")


    def _extract_session_id(self, request: PendingRequest):
        request_id = None
        
        # Extract request_id from the request args (ChatCompletionRequest or CompletionRequest)
        for arg in request.args:
            if hasattr(arg, 'request_id') and arg.request_id:
                request_id = arg.request_id
                break
        
        if request_id:
            session_id = get_request_session_mapping(request_id)
            if session_id:
                logger.info(f"Session ID extracted from request: session_id={session_id}, request_id={request_id}")
                # Delete the mapping after use to clean up
                delete_request_session_mapping(request_id)
                return session_id
        

    def _find_matched_replica(self, candidate_replicas: List[RunningReplica], pending_request: Optional[PendingRequest]) -> Optional[RunningReplica]:
        """
        Find the replica that matches the session-id in the request.
        """
        if not pending_request:
            return 
        
        session_id = self._extract_session_id(pending_request)
        if session_id:
            logger.info(f"Looking for replica with session ID: {session_id}")
            for replica in candidate_replicas:
                routing_stats = replica.routing_stats
                if not routing_stats:
                    continue
                hot_sessions = routing_stats.get('hot_sessions', set())
                if session_id in hot_sessions:
                    logger.info(f"Found matching replica for session ID {session_id}: replica_id={replica.replica_tag}")
                    return replica
            logger.info(f"No matching replica found for session ID: {session_id}")
        return None
    
    
    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:

        # Update our session mapping from latest replica stats
        matched_replica = self._find_matched_replica(candidate_replicas, pending_request)
        
        if matched_replica:
            logger.info(f"Using session-aware routing: replica_id={matched_replica.replica_tag}")
            return [[matched_replica]]

        # Fallback to PowerOfTwoChoicesRequestRouter
        return await super().choose_replicas(candidate_replicas, pending_request)
    