import math
import os
from flask import Flask, send_from_directory
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
from routes.metrics import metrics_bp
from routes.filters import filters_bp


# ---------------------------------------------------------------------------
# NaN-safe JSON provider
# ---------------------------------------------------------------------------
class _SafeJSONProvider(DefaultJSONProvider):
    """Serialise float NaN / Infinity as null instead of invalid JSON."""

    def default(self, o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        return super().default(o)

    def dumps(self, obj, **kwargs):
        obj = _clean(obj)
        return super().dumps(obj, **kwargs)


def _clean(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
# Resolve the web_client folder relative to this file's location
_WEB_ROOT = os.path.join(os.path.dirname(__file__), "..", "web_client")
_WEB_ROOT = os.path.abspath(_WEB_ROOT)

app = Flask(__name__, static_folder=None)
app.json_provider_class = _SafeJSONProvider
app.json = _SafeJSONProvider(app)

CORS(app)

app.register_blueprint(metrics_bp, url_prefix="/metrics")
app.register_blueprint(filters_bp, url_prefix="/filter")


# ---------------------------------------------------------------------------
# Serve web_client as static files
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    """Redirect root to the summary page."""
    return send_from_directory(
        os.path.join(_WEB_ROOT, "components"), "summary.html"
    )


@app.route("/transactions")
def transactions():
    return send_from_directory(
        os.path.join(_WEB_ROOT, "components"), "transactions.html"
    )


@app.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory(os.path.join(_WEB_ROOT, "assets"), filename)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.route("/status")
def status():
    return {"status": "API running"}


if __name__ == "__main__":
    app.run(debug=True)
