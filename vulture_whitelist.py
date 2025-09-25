# Vulture whitelist for legitimate unused code
# This file contains code that vulture thinks is unused but is actually needed

# Exception handling variables that are intentionally unused
# These are common in except blocks where we only care about the exception type
exc_type = None  # Used in exception handlers
exc_val = None   # Used in exception handlers  
exc_tb = None    # Used in exception handlers

# Template environment variable - may be used by Jinja2 internally
environment = None

# Database exception imports that may be used conditionally
IntegrityError = None
ProgrammingError = None
VerticaConnectionError = None

# CLI parameter variable that might be used in decorators or callbacks
param = None