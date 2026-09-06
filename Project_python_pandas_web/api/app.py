import math
from flask import Flask
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
from routes.metrics import metrics_bp
from routes.filters import filters_bp


class _SafeJSONProvider(DefaultJSONProvider):
    """Extend Flask's JSON provider to serialise float NaN/Infinity as null."""

    def default(self, o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        return super().default(o)

    def dumps(self, obj, **kwargs):
        # Walk the object tree and replace NaN floats before serialising
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


app = Flask(__name__)
app.json_provider_class = _SafeJSONProvider
app.json = _SafeJSONProvider(app)

CORS(app)

app.register_blueprint(metrics_bp, url_prefix="/metrics")
app.register_blueprint(filters_bp, url_prefix="/filter")


@app.route("/")
def home():
    return {"status": "API running"}


if __name__ == "__main__":
    app.run(debug=True)
