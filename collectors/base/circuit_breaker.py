#!/usr/bin/env python3
"""
Circuit Breaker Pattern Implementation
Prevents cascade failures by temporarily blocking calls to failing services
"""

import time
import logging
from enum import Enum
from typing import Callable, Any, Optional
from threading import Lock

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""

    CLOSED = "CLOSED"  # Normal operation
    OPEN = "OPEN"  # Blocking calls due to failures
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered


class CircuitOpenError(Exception):
    """Raised when circuit is open and calls are blocked"""

    pass


class CircuitBreaker:
    """
    Circuit Breaker implementation with configurable thresholds

    States:
    - CLOSED: Normal operation, calls pass through
    - OPEN: Too many failures, calls are blocked
    - HALF_OPEN: Testing recovery with limited calls
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception,
        success_threshold: int = 2,
    ):
        """
        Initialize circuit breaker

        Args:
            name: Circuit breaker identifier
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to catch
            success_threshold: Successes needed in HALF_OPEN to close circuit
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.success_threshold = success_threshold

        # State management
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.last_state_change: float = time.time()

        # Thread safety
        self._lock = Lock()

        # Metrics
        self.total_calls = 0
        self.total_failures = 0
        self.total_successes = 0
        self.circuit_opens = 0

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function through circuit breaker

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            CircuitOpenError: If circuit is open
            Exception: If function fails
        """
        with self._lock:
            self.total_calls += 1

            # Check circuit state
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    error_msg = (
                        f"Circuit {self.name} is OPEN. "
                        f"Retry after {self._time_until_retry():.0f} seconds"
                    )
                    logger.warning(error_msg)
                    raise CircuitOpenError(error_msg)

        # Execute function
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except self.expected_exception:
            self._on_failure()
            raise

    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute async function through circuit breaker

        Args:
            func: Async function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            CircuitOpenError: If circuit is open
            Exception: If function fails
        """
        with self._lock:
            self.total_calls += 1

            # Check circuit state
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    error_msg = (
                        f"Circuit {self.name} is OPEN. "
                        f"Retry after {self._time_until_retry():.0f} seconds"
                    )
                    logger.warning(error_msg)
                    raise CircuitOpenError(error_msg)

        # Execute async function
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result

        except self.expected_exception:
            self._on_failure()
            raise

    def _on_success(self):
        """Handle successful call"""
        with self._lock:
            self.total_successes += 1

            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                logger.info(
                    f"Circuit {self.name} success in HALF_OPEN "
                    f"({self.success_count}/{self.success_threshold})"
                )

                if self.success_count >= self.success_threshold:
                    self._transition_to_closed()

            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = 0

    def _on_failure(self):
        """Handle failed call"""
        with self._lock:
            self.total_failures += 1
            self.failure_count += 1
            self.last_failure_time = time.time()

            logger.warning(
                f"Circuit {self.name} failure "
                f"({self.failure_count}/{self.failure_threshold})"
            )

            if self.state == CircuitState.HALF_OPEN:
                # Single failure in HALF_OPEN reopens circuit
                self._transition_to_open()

            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    self._transition_to_open()

    def _should_attempt_reset(self) -> bool:
        """Check if enough time passed to attempt reset"""
        if self.last_failure_time is None:
            return False

        return time.time() - self.last_failure_time >= self.recovery_timeout

    def _time_until_retry(self) -> float:
        """Calculate seconds until retry is allowed"""
        if self.last_failure_time is None:
            return 0

        elapsed = time.time() - self.last_failure_time
        return max(0, self.recovery_timeout - elapsed)

    def _transition_to_open(self):
        """Transition to OPEN state"""
        self.state = CircuitState.OPEN
        self.last_state_change = time.time()
        self.circuit_opens += 1

        logger.error(
            f"Circuit {self.name} transitioned to OPEN. "
            f"Will retry in {self.recovery_timeout} seconds"
        )

    def _transition_to_half_open(self):
        """Transition to HALF_OPEN state"""
        self.state = CircuitState.HALF_OPEN
        self.last_state_change = time.time()
        self.success_count = 0
        self.failure_count = 0

        logger.info(
            f"Circuit {self.name} transitioned to HALF_OPEN. "
            f"Testing with {self.success_threshold} calls"
        )

    def _transition_to_closed(self):
        """Transition to CLOSED state"""
        self.state = CircuitState.CLOSED
        self.last_state_change = time.time()
        self.failure_count = 0
        self.success_count = 0

        logger.info(
            f"Circuit {self.name} transitioned to CLOSED. " f"Service recovered"
        )

    def reset(self):
        """Manually reset circuit to CLOSED state"""
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            self.last_state_change = time.time()

            logger.info(f"Circuit {self.name} manually reset to CLOSED")

    def get_status(self) -> dict:
        """Get circuit breaker status"""
        with self._lock:
            status = {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "total_calls": self.total_calls,
                "total_failures": self.total_failures,
                "total_successes": self.total_successes,
                "circuit_opens": self.circuit_opens,
                "last_failure_time": self.last_failure_time,
                "last_state_change": self.last_state_change,
                "time_until_retry": (
                    self._time_until_retry() if self.state == CircuitState.OPEN else 0
                ),
            }

            return status

    def __str__(self) -> str:
        """String representation"""
        return f"CircuitBreaker({self.name}, state={self.state.value})"


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers"""

    def __init__(self):
        self.breakers: dict[str, CircuitBreaker] = {}
        self._lock = Lock()

    def register(self, breaker: CircuitBreaker):
        """Register a circuit breaker"""
        with self._lock:
            self.breakers[breaker.name] = breaker
            logger.info(f"Registered circuit breaker: {breaker.name}")

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit breaker by name"""
        return self.breakers.get(name)

    def get_or_create(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        **kwargs,
    ) -> CircuitBreaker:
        """Get existing or create new circuit breaker"""
        with self._lock:
            if name not in self.breakers:
                breaker = CircuitBreaker(
                    name=name,
                    failure_threshold=failure_threshold,
                    recovery_timeout=recovery_timeout,
                    **kwargs,
                )
                self.breakers[name] = breaker

            return self.breakers[name]

    def reset_all(self):
        """Reset all circuit breakers"""
        with self._lock:
            for breaker in self.breakers.values():
                breaker.reset()

    def get_all_status(self) -> list[dict]:
        """Get status of all circuit breakers"""
        with self._lock:
            return [breaker.get_status() for breaker in self.breakers.values()]

    def get_open_circuits(self) -> list[str]:
        """Get names of open circuits"""
        with self._lock:
            return [
                name
                for name, breaker in self.breakers.items()
                if breaker.state == CircuitState.OPEN
            ]


# Global registry instance
registry = CircuitBreakerRegistry()


def circuit_breaker(
    name: Optional[str] = None,
    failure_threshold: int = 5,
    recovery_timeout: int = 60,
    expected_exception: type = Exception,
):
    """
    Decorator for applying circuit breaker to functions

    Usage:
        @circuit_breaker(name="api_call", failure_threshold=3)
        def make_api_call():
            # API call logic
            pass
    """

    def decorator(func: Callable) -> Callable:
        breaker_name = name or f"{func.__module__}.{func.__name__}"
        breaker = registry.get_or_create(
            breaker_name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
        )

        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)

        async def async_wrapper(*args, **kwargs):
            return await breaker.call_async(func, *args, **kwargs)

        # Return appropriate wrapper based on function type
        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper

    return decorator


# Example usage and testing
if __name__ == "__main__":
    import random
    import asyncio

    # Example 1: Simple function with circuit breaker
    @circuit_breaker(name="unreliable_api", failure_threshold=3, recovery_timeout=10)
    def unreliable_api_call():
        if random.random() < 0.7:  # 70% failure rate
            raise ConnectionError("API unavailable")
        return "Success!"

    # Example 2: Async function with circuit breaker
    @circuit_breaker(name="async_api", failure_threshold=2)
    async def async_api_call():
        await asyncio.sleep(0.1)
        if random.random() < 0.5:  # 50% failure rate
            raise TimeoutError("Request timeout")
        return "Async success!"

    # Test synchronous circuit breaker
    print("Testing synchronous circuit breaker:")
    for i in range(10):
        try:
            result = unreliable_api_call()
            print(f"Call {i+1}: {result}")
        except CircuitOpenError as e:
            print(f"Call {i+1}: Circuit open - {e}")
        except Exception as e:
            print(f"Call {i+1}: Failed - {e}")
        time.sleep(1)

    # Print circuit status
    print("\nCircuit breaker status:")
    for status in registry.get_all_status():
        print(
            f"  {status['name']}: {status['state']} "
            f"(failures: {status['total_failures']}, "
            f"successes: {status['total_successes']})"
        )
