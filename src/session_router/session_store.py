import ray
from typing import Optional, Dict
from fastapi import Request


@ray.remote
class SessionManager:
    def __init__(self):
        self._request_session_mappings: Dict[str, str] = {}
        self._session_replica_mappings: Dict[str, str] = {}
    
    def store_mapping(self, request_id: str, session_id: str) -> None:
        """Store a request ID to session ID mapping."""
        self._request_session_mappings[request_id] = session_id
    
    def get_mapping(self, request_id: str) -> Optional[str]:
        """Get session ID for a given request ID."""
        return self._request_session_mappings.get(request_id)
    
    def delete_mapping(self, request_id: str) -> None:
        """Delete a request ID to session ID mapping."""
        self._request_session_mappings.pop(request_id, None)
    
    def get_replica_for_session(self, session_id: str) -> Optional[str]:
        """Get replica ID for a given session ID."""
        return self._session_replica_mappings.get(session_id)
    
    def associate_session_with_replica(self, session_id: str, replica_id: str) -> None:
        """Associate a session ID with a replica ID."""
        self._session_replica_mappings[session_id] = replica_id

    # We also need to handle the case where the replica handling a session dies
    

SESSION_MANAGER_ACTOR_NAME = "session_manager"

def get_session_manager():
    """Get or create the detached session manager actor."""
    try:
        # Try to get existing detached actor
        return ray.get_actor(SESSION_MANAGER_ACTOR_NAME)
    except ValueError:
        # Actor doesn't exist, create a new detached one
        return SessionManager.options(name=SESSION_MANAGER_ACTOR_NAME, lifetime="detached").remote()


def store_request_session_mapping(request_id: str, session_id: str) -> None:
    """Store a request ID to session ID mapping."""
    manager = get_session_manager()
    ray.get(manager.store_mapping.remote(request_id, session_id))


def get_request_session_mapping(request_id: str) -> Optional[str]:
    """Get session ID for a given request ID."""
    manager = get_session_manager()
    return ray.get(manager.get_mapping.remote(request_id))


def delete_request_session_mapping(request_id: str) -> None:
    """Delete a request ID to session ID mapping."""
    manager = get_session_manager()
    ray.get(manager.delete_mapping.remote(request_id))


def get_replica_for_session(session_id: str) -> Optional[str]:
    """Get replica ID for a given session ID."""
    manager = get_session_manager()
    return ray.get(manager.get_replica_for_session.remote(session_id))


def associate_session_with_replica(session_id: str, replica_id: str) -> None:
    """Associate a session ID with a replica ID."""
    manager = get_session_manager()
    ray.get(manager.associate_session_with_replica.remote(session_id, replica_id))


def extract_session_id_from_cookie(request: Request) -> Optional[str]:
    """Extract session ID from the route cookie field."""
    cookie_header = request.headers.get('cookie', '')
    if not cookie_header:
        return None
    
    # Parse cookies to find the route field
    for cookie_part in cookie_header.split(';'):
        cookie_part = cookie_part.strip()
        if cookie_part.startswith('route='):
            session_id = cookie_part.removeprefix('route=')
            return session_id
    
    return None
