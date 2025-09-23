import ray
from typing import Optional, Dict
from fastapi import Request


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
