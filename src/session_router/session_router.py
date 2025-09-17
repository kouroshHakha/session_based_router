
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.pow_2_router import PowerOfTwoChoicesRequestRouter
from ray.serve._private.common import ReplicaID
from ray.serve.handle import ReplicaResult

from typing import List, Optional
from .session_store import get_replica_for_session, associate_session_with_replica

import logging
logger = logging.getLogger(__name__)

class SessionAwareRequestRouter(PowerOfTwoChoicesRequestRouter):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"[DEBUG] SessionAwareRequestRouter initialized.")


    def _extract_session_id(self, request: PendingRequest, delete_mapping: bool = False):
        request_id = None
        
        # Extract request_id from the request args (ChatCompletionRequest or CompletionRequest)
        for arg in request.args:
            if hasattr(arg, 'request_id') and arg.request_id:
                request_id = arg.request_id
                break
        
        if request_id:
            # session_id = get_request_session_mapping(request_id)
            session_id = arg.vllm_xargs.get("session_id")
            if session_id:
                logger.info(f"Session ID extracted from request: session_id={session_id}, request_id={request_id}")
                # if delete_mapping:
                #     # Delete the mapping after use to clean up
                #     delete_request_session_mapping(request_id)
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
            
            # Check if we have a stored mapping
            replica_id = get_replica_for_session(session_id)
            if replica_id:
                logger.info(f"Found stored replica mapping for session {session_id}: replica_id={replica_id}")
                # Find the replica with this ID among candidates
                for replica in candidate_replicas:
                    if replica.replica_id.to_full_id_str() == replica_id:
                        logger.info(f"Found matching replica from stored mapping: replica_id={replica.replica_id}")
                        return replica
                logger.info(f"Stored replica {replica_id} not in candidate list")
            
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
            logger.info(f"Using session-aware routing: replica_id={matched_replica.replica_id}")
            return [[matched_replica]]

        # Fallback to PowerOfTwoChoicesRequestRouter
        return await super().choose_replicas(candidate_replicas, pending_request)
    
    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        """Called when a request is routed to a replica.
        
        Associates the session ID with the replica ID if they don't already have a mapping.
        """
        session_id = self._extract_session_id(pending_request, delete_mapping=True)
        replica_id_str = replica_id.to_full_id_str()
        if session_id:
            # Check if we already have a mapping for this session
            existing_replica = get_replica_for_session(session_id)
            if not existing_replica:
                # Associate this session with the replica it was routed to
                logger.info(f"Associating session {session_id} with replica {replica_id_str}")
                associate_session_with_replica(session_id, replica_id_str)
            else:
                logger.info(f"Session {session_id} already associated with replica {existing_replica}")
        
        # Call parent implementation
        super().on_request_routed(pending_request, replica_id, result)
