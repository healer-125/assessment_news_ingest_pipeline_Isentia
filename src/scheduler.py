import logging
import time
from typing import Callable, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class Scheduler:
    """Simple scheduler for periodic task execution."""
    
    def __init__(self, interval_seconds: int):
        """
        Initialize scheduler.
        
        Args:
            interval_seconds: Interval between executions in seconds
        """
        self.interval_seconds = interval_seconds
        self.running = False
    
    def run_periodic(self, task: Callable, max_iterations: Optional[int] = None):
        """
        Run a task periodically.
        
        Args:
            task: Callable to execute
            max_iterations: Maximum number of iterations (None for infinite)
        """
        self.running = True
        iteration = 0
        
        logger.info(f"Starting scheduler with {self.interval_seconds}s interval")
        
        while self.running:
            try:
                iteration += 1
                logger.info(f"Starting iteration {iteration} at {datetime.utcnow().isoformat()}")
                
                task()
                
                if max_iterations and iteration >= max_iterations:
                    logger.info(f"Reached max iterations ({max_iterations}), stopping")
                    break
                
                logger.info(f"Waiting {self.interval_seconds} seconds until next iteration")
                time.sleep(self.interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping scheduler")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in scheduled task: {str(e)}")
                logger.info(f"Waiting {self.interval_seconds} seconds before retry")
                time.sleep(self.interval_seconds)
    
    def stop(self):
        """Stop the scheduler."""
        self.running = False
