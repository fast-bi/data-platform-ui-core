from flask_login import UserMixin
import logging

class CustomUser(UserMixin):
    def __init__(self, user_id, username, email):
        self.id = user_id
        self.username = username
        self.email = email

    def get_id(self):
        """Return the email address to satisfy Flask-Login's requirements."""
        return self.id

    # Optional, depending on your application's needs
    def is_authenticated(self):
        """Always return True, as all logged-in users are authenticated."""
        return True

    def is_active(self):
        """Always return True, as all users are active by default."""
        return True

    def is_anonymous(self):
        """Always return False, as anonymous users aren't supported."""
        return False

class User:
    def __init__(self, id, username, email, first_login_time, last_login_time, follow_mode, iframe_mode, light_dark_mode):
        self.id = id
        self.username = username
        self.email = email
        self.first_login_time = first_login_time
        self.last_login_time = last_login_time
        self.follow_mode = follow_mode
        self.iframe_mode = iframe_mode
        self.light_dark_mode = light_dark_mode

class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass