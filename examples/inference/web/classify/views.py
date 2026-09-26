"""The views: the page with the form, and the JSON endpoint. Both send the
uploaded image to a worker through the multiplexers and turn each way the
library can fail into a status code the caller can act on."""

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.threaded_client import BackendError

from classify.mx import mx
from inference_pb2 import InferRequest, InferResponse
from multiplexer_constants import types

# What each failure means, for the status code and the message: no live
# multiplexer at all; no worker anywhere, or the one asked left; the answer
# did not come in time; the worker's own code failed.
FAILURES = {
    NotConnected: (503, "no multiplexer is reachable"),
    OperationFailed: (503, "no worker is available"),
    OperationTimedOut: (504, "the worker did not answer in time"),
    BackendError: (502, "the worker failed on this image"),
}


def infer(image: bytes) -> InferResponse:
    """The worker's answer for an image; raises what query() raises."""
    reply = mx().query(InferRequest(image=image), type=types.INFER_REQUEST, timeout=settings.INFER_TIMEOUT)
    response = InferResponse()
    response.ParseFromString(reply.message)
    return response


def as_dict(response: InferResponse) -> dict:
    """The answer as the JSON endpoint returns it."""
    return {
        "label": response.label,
        "probability": response.probabilities[response.label],
        "probabilities": list(response.probabilities),
        "worker": response.worker,
        "model_seconds": response.seconds,
    }


def too_large(size: int | None) -> str | None:
    """Why an upload of `size` bytes is refused, or None when it is not."""
    if size is not None and size > settings.MAX_IMAGE_BYTES:
        return f"the image is over {settings.MAX_IMAGE_BYTES} bytes"
    return None


@csrf_exempt
def classify(request: HttpRequest) -> HttpResponse:
    """POST an image as the `image` field of a multipart form; JSON back."""
    upload = request.FILES.get("image")
    if request.method != "POST" or upload is None:
        return JsonResponse({"error": "POST an image as the multipart field 'image'"}, status=400)
    refusal = too_large(upload.size)
    if refusal:
        return JsonResponse({"error": refusal}, status=413)
    try:
        return JsonResponse(as_dict(infer(upload.read())))
    except tuple(FAILURES) as error:
        status, message = FAILURES[type(error)]
        return JsonResponse({"error": message, "exception": type(error).__name__}, status=status)


def index(request: HttpRequest) -> HttpResponse:
    """The page: an upload form, and the answer for the image just sent."""
    context: dict = {}
    upload = request.FILES.get("image")
    if request.method == "POST" and upload is not None:
        refusal = too_large(upload.size)
        if refusal:
            context["error"] = refusal
        else:
            try:
                context["result"] = as_dict(infer(upload.read()))
            except tuple(FAILURES) as error:
                context["error"] = FAILURES[type(error)][1]
    return render(request, "classify/index.html", context)
