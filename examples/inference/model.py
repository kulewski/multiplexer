"""The model of the inference example: a small convolutional network that
reads a handwritten digit off a 28x28 grey image, trained on MNIST by
train.py in under a minute on a CPU, with its weights committed as
weights.pt. What matters to the example is not the model but the shape of
this module: load() once at start, predict() per request, nothing else.
For a real model the two functions change and nothing around them does."""

import io
import os

import torch
from PIL import Image, ImageOps
from torch import nn

WEIGHTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weights.pt")
MEAN, STD = 0.1307, 0.3081  # of MNIST's pixels, the normalisation the model was trained with


class DigitNet(nn.Module):
    """Two convolutions, two poolings, one linear layer: about nine
    thousand parameters, about 97% on MNIST's test set after one epoch."""

    def __init__(self) -> None:
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(1, 8, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(8, 16, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
        )
        self.classifier = nn.Linear(16 * 7 * 7, 10)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Logits for a batch of 1x28x28 images."""
        return self.classifier(self.features(x).flatten(1))


def load(path: str = WEIGHTS, device: str = "cpu") -> DigitNet:
    """The trained model, in evaluation mode, on `device`."""
    model = DigitNet()
    model.load_state_dict(torch.load(path, map_location=device))
    return model.to(device).eval()


def to_tensor(image_bytes: bytes) -> torch.Tensor:
    """An image file's bytes as the 1x1x28x28 tensor the model reads: grey,
    resized, and inverted when the digit is dark on light, since MNIST's
    digits are light on dark."""
    image = Image.open(io.BytesIO(image_bytes)).convert("L").resize((28, 28))
    if sum(image.tobytes()) / (28 * 28) > 127:
        image = ImageOps.invert(image)
    pixels = torch.tensor(list(image.tobytes()), dtype=torch.float32).view(1, 1, 28, 28) / 255.0
    return (pixels - MEAN) / STD


def predict(model: DigitNet, image_bytes: bytes) -> tuple[int, list[float]]:
    """The digit the model reads in the image, and the probability of each digit."""
    device = next(model.parameters()).device  # the model's, a GPU's when train.py loaded it there
    with torch.no_grad():
        probabilities = torch.softmax(model(to_tensor(image_bytes).to(device)), dim=1)[0]
    return int(probabilities.argmax()), [float(p) for p in probabilities]
