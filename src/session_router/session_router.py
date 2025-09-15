

from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.pow_2_router import PowerOfTwoChoicesRequestRouter

from typing import List, Optional

class SessionAwareRequestRouter(PowerOfTwoChoicesRequestRouter):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"[DEBUG] SessionAwareRequestRouter initialized. Testing ....")


    def _extract_session_id(self, request: PendingRequest):
        for arg in request.args:
            xargs = None
            if isinstance(arg, dict):
                xargs = arg.get("vllm_xargs", {})
            else:
                xargs = getattr(arg, "vllm_xargs", {})
            if xargs: 
                return xargs.get("session_id")
    
    
    def _find_matched_replica(self, candidate_replicas: List[RunningReplica], pending_request: Optional[PendingRequest]) -> Optional[RunningReplica]:
        """
        Find the replica that matches the session-id in the request.
        """
        if not pending_request:
            return 
        
        session_id = self._extract_session_id(pending_request)
        if session_id:
            for replica in candidate_replicas:
                routing_stats = replica.routing_stats
                if not routing_stats:
                    continue
                
                hot_sessions = routing_stats.get('hot_sessions', set())
                if session_id in hot_sessions:
                    print(f"[DEBUG] Found a match for session_id: {session_id}, on replica {replica.replica_id.unique_id}")
                    return replica
        print(f"[DEBUG] No session id found for request: {pending_request}")
        return None
    
    
    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:

        # Update our session mapping from latest replica stats
        matched_replica = self._find_matched_replica(candidate_replicas, pending_request)
        
        if matched_replica:
            return [[matched_replica]]

        # Fallback to PowerOfTwoChoicesRequestRouter
        return await super().choose_replicas(candidate_replicas, pending_request)
    