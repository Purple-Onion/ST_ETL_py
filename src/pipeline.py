"""Main ETL pipeline orchestrator."""
import logging
from typing import Any, Callable, List, Optional
from dataclasses import dataclass, field

from .extract.base import BaseExtractor
from .transform.base import BaseTransformer
from .load.base import BaseLoader

logger = logging.getLogger(__name__)


@dataclass
class PipelineStep:
    """Represents a step in the ETL pipeline."""
    name: str
    func: Callable
    on_error: str = "fail"  # "fail", "skip", "retry"
    retries: int = 3


class ETLPipeline:
    """Orchestrate ETL operations."""
    
    def __init__(self, name: str = "etl_pipeline"):
        self.name = name
        self.logger = logging.getLogger(f"Pipeline.{name}")
        self._extractors: List[BaseExtractor] = []
        self._transformers: List[BaseTransformer] = []
        self._loaders: List[BaseLoader] = []
        self._pre_hooks: List[Callable] = []
        self._post_hooks: List[Callable] = []
    
    def add_extractor(self, extractor: BaseExtractor) -> 'ETLPipeline':
        """Add an extractor to the pipeline."""
        self._extractors.append(extractor)
        return self
    
    def add_transformer(self, transformer: BaseTransformer) -> 'ETLPipeline':
        """Add a transformer to the pipeline."""
        self._transformers.append(transformer)
        return self
    
    def add_loader(self, loader: BaseLoader) -> 'ETLPipeline':
        """Add a loader to the pipeline."""
        self._loaders.append(loader)
        return self
    
    def add_pre_hook(self, hook: Callable) -> 'ETLPipeline':
        """Add a pre-execution hook."""
        self._pre_hooks.append(hook)
        return self
    
    def add_post_hook(self, hook: Callable) -> 'ETLPipeline':
        """Add a post-execution hook."""
        self._post_hooks.append(hook)
        return self
    
    def run(self, **extract_kwargs) -> Any:
        """Execute the ETL pipeline.
        
        Args:
            **extract_kwargs: Arguments passed to extractors
            
        Returns:
            Final transformed data
        """
        self.logger.info(f"Starting pipeline: {self.name}")
        
        # Pre-hooks
        for hook in self._pre_hooks:
            hook()
        
        try:
            # Extract
            data = self._run_extract(**extract_kwargs)
            
            # Transform
            data = self._run_transform(data)
            
            # Load
            self._run_load(data)
            
            self.logger.info(f"Pipeline completed: {self.name}")
            
            # Post-hooks
            for hook in self._post_hooks:
                hook()
            
            return data
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
    
    def _run_extract(self, **kwargs) -> Any:
        """Run all extractors."""
        if not self._extractors:
            raise ValueError("No extractors configured")
        
        # For single extractor, return data directly
        if len(self._extractors) == 1:
            extractor = self._extractors[0]
            with extractor:
                return extractor.extract(**kwargs)
        
        # For multiple extractors, return list of data
        results = []
        for extractor in self._extractors:
            with extractor:
                results.append(extractor.extract(**kwargs))
        return results
    
    def _run_transform(self, data: Any) -> Any:
        """Run all transformers."""
        result = data
        for transformer in self._transformers:
            result = transformer.transform(result)
        return result
    
    def _run_load(self, data: Any) -> None:
        """Run all loaders."""
        for loader in self._loaders:
            with loader:
                loader.load(data)
