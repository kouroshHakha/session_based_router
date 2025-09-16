import ray
from typing import Optional, Dict
from fastapi import Request


@ray.remote
class SessionStore:
    def __init__(self):
        self._request_session_mappings: Dict[str, str] = {}
    
    def store_mapping(self, request_id: str, session_id: str) -> None:
        """Store a request ID to session ID mapping."""
        self._request_session_mappings[request_id] = session_id
    
    def get_mapping(self, request_id: str) -> Optional[str]:
        """Get session ID for a given request ID."""
        return self._request_session_mappings.get(request_id)
    
    def delete_mapping(self, request_id: str) -> None:
        """Delete a request ID to session ID mapping."""
        self._request_session_mappings.pop(request_id, None)
    
    def get_all_mappings(self) -> Dict[str, str]:
        """Get all current mappings (for debugging)."""
        return self._request_session_mappings.copy()


SESSION_STORE_ACTOR_NAME = "session_store"

def get_session_store():
    """Get or create the detached session store actor."""
    try:
        # Try to get existing detached actor
        return ray.get_actor(SESSION_STORE_ACTOR_NAME)
    except ValueError:
        # Actor doesn't exist, create a new detached one
        return SessionStore.options(name=SESSION_STORE_ACTOR_NAME, lifetime="detached").remote()


def store_request_session_mapping(request_id: str, session_id: str) -> None:
    """Store a request ID to session ID mapping."""
    store = get_session_store()
    ray.get(store.store_mapping.remote(request_id, session_id))


def get_request_session_mapping(request_id: str) -> Optional[str]:
    """Get session ID for a given request ID."""
    store = get_session_store()
    return ray.get(store.get_mapping.remote(request_id))


def delete_request_session_mapping(request_id: str) -> None:
    """Delete a request ID to session ID mapping."""
    store = get_session_store()
    ray.get(store.delete_mapping.remote(request_id))


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
