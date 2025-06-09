import logging
import threading
import time


class RaiseOnErrorHandler(logging.Handler):
    def __init__(self, flush_delay=1.0):
        """
        Initializes the error handler with a delay before raising an exception.
        :param flush_delay: Time in seconds to wait for additional error messages before raising.
        """
        super().__init__()
        self.error_messages = []  # Stores accumulated error messages
        self.flush_delay = flush_delay  # Delay before raising an error
        self.flush_timer = None  # Timer object
        self.exceptions = []  # Store the exception for the main thread

    def flush_errors(self):
        """Stores the exception instead of raising it inside the thread."""
        if self.error_messages:
            full_message = "\n".join(self.error_messages)
            self.error_messages = []  # Reset accumulator
            self.exceptions.append(RuntimeError(f"yfinance ERROR:\n{full_message}"))  # Store exception

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self.error_messages.append(record.getMessage())  # Store error message

            # Cancel existing timer if another error arrives within the delay period
            if self.flush_timer and self.flush_timer.is_alive():
                self.flush_timer.cancel()

            # Start a new flush timer
            self.flush_timer = threading.Timer(self.flush_delay, self.flush_errors)
            self.flush_timer.start()


    def check_for_exception(self):
        """Raises the stored exception in the main thread if it exists."""
        if len(self.exceptions) > 0:
            for exc in self.exceptions:
                raise exc  # Raise in the main thread

        self.exceptions = []

    def get_exceptions(self):
        """Returns the stored exceptions."""
        return self.exceptions

    def clean_exceptions(self):
        """Clears the stored exceptions."""
        self.exceptions = []