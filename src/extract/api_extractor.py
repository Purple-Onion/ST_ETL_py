"""API extractor for REST endpoints."""
import requests
from typing import Any, Dict, List, Optional, Union
from .base import BaseExtractor


class APIExtractor(BaseExtractor):
    """Extract data from REST APIs."""
    
    def __init__(
        self, 
        base_url: str, 
        headers: Optional[Dict[str, str]] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(config)
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.session: Optional[requests.Session] = None
    
    def connect(self) -> None:
        """Initialize HTTP session."""
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.logger.info(f"Connected to API: {self.base_url}")
    
    def extract(
        self, 
        endpoint: str = "", 
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Union[Dict, List]:
        """Extract data from API endpoint.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (GET, POST, etc.)
            params: Query parameters
            json_data: JSON body for POST requests
            **kwargs: Additional arguments passed to requests
            
        Returns:
            Response data as dict or list
        """
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_data,
            **kwargs
        )
        response.raise_for_status()
        
        data = response.json()
        self.logger.info(f"Extracted data from {url}")
        return data
    
    def disconnect(self) -> None:
        """Close HTTP session."""
        if self.session:
            self.session.close()
            self.session = None
        self.logger.info("Disconnected from API")
