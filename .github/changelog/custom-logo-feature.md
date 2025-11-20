### Custom Logo Feature

**Added:**
-   **Backend**: New endpoints `/ui-config/upload_logo`, `/ui-config/custom_logo`, and `/ui-config/logo_status` to handle custom company logo upload and serving.
-   **Backend**: Context processor `inject_logo_path` to dynamically serve the correct logo path (custom or default) to Jinja2 templates, preventing UI flicker.
-   **Frontend**: Long-press (15 seconds) functionality on the main logo to trigger an upload modal for Administrators.
-   **Frontend**: AJAX handling for logo file upload.
-   **UI**: Updated all HTML templates to use the dynamic `{{ logo_path }}` variable instead of hardcoded image sources.

**Changed:**
-   Refactored logo image source in `home.html`, `index.html`, and all iframe templates to support dynamic loading.
-   Ensured logo upload is restricted to users with 'Admin' or 'consoleAdmin' group membership.

