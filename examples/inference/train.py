"""Trains the example's model on MNIST and writes weights.pt; also writes
one sample image per digit for the client to send. One epoch on a CPU
takes a few seconds and reaches about 97% on the test set.

    python train.py [--epochs 1] [--data data] [--weights weights.pt] [--samples samples]

The dataset, 12 MB, is downloaded into --data on the first run. The
walkthrough runs this once; the committed weights.pt and samples/ mean
nobody else has to."""

import argparse
import io
import os
import time

import torch
from torch import nn
from torchvision import datasets, transforms

from model import MEAN, STD, WEIGHTS, DigitNet, load, predict

HERE = os.path.dirname(os.path.abspath(__file__))


def data(data_dir: str, train: bool) -> torch.utils.data.DataLoader:
    """MNIST, normalised as the model expects, in batches."""
    dataset = datasets.MNIST(
        data_dir,
        train=train,
        download=True,
        transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((MEAN,), (STD,))]),
    )
    return torch.utils.data.DataLoader(dataset, batch_size=64, shuffle=train)


def accuracy(model: DigitNet, loader: torch.utils.data.DataLoader, device: str) -> float:
    """The share of images the model reads correctly."""
    correct = total = 0
    with torch.no_grad():
        for images, labels in loader:
            correct += int((model(images.to(device)).argmax(1) == labels.to(device)).sum())
            total += len(labels)
    return correct / total


def train(epochs: int, data_dir: str, weights: str, device: str) -> float:
    """Train for `epochs`, save the weights, return the test accuracy."""
    torch.manual_seed(0)
    model = DigitNet().to(device)
    optimiser = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_function = nn.CrossEntropyLoss()
    training, test = data(data_dir, train=True), data(data_dir, train=False)
    for epoch in range(epochs):
        model.train()
        started = time.monotonic()
        for step, (images, labels) in enumerate(training):
            optimiser.zero_grad()
            loss = loss_function(model(images.to(device)), labels.to(device))
            loss.backward()
            optimiser.step()
            if step % 300 == 0:
                print(f"epoch {epoch + 1} step {step:4d} loss {loss.item():.3f}", flush=True)
        model.eval()
        print(
            f"epoch {epoch + 1} done in {time.monotonic() - started:.0f} s, test accuracy {accuracy(model, test, device):.4f}",
            flush=True,
        )
    torch.save(model.state_dict(), weights)
    print(f"weights saved to {weights}, {os.path.getsize(weights)} bytes")
    return accuracy(model, test, device)


def write_samples(data_dir: str, samples_dir: str, weights: str, device: str) -> None:
    """One test-set image per digit, as samples/<digit>.png, for the client
    to send: the first of each digit the trained model reads with confidence,
    so that the walkthrough's answers are about the system, not the model."""
    os.makedirs(samples_dir, exist_ok=True)
    model = load(weights, device)
    dataset = datasets.MNIST(data_dir, train=False, download=True)
    wanted = set(range(10))
    for image, label in dataset:
        if label not in wanted:
            continue
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        read, probabilities = predict(model, buffer.getvalue())
        if read == label and probabilities[label] > 0.99:
            image.save(os.path.join(samples_dir, f"{label}.png"))
            wanted.remove(label)
        if not wanted:
            break
    print(f"samples written to {samples_dir}")


def main() -> None:
    """Train, save, write the samples."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--data", default=os.path.join(HERE, "data"), help="where MNIST is downloaded to")
    parser.add_argument("--weights", default=WEIGHTS)
    parser.add_argument("--samples", default=os.path.join(HERE, "samples"))
    parser.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    args = parser.parse_args()
    train(args.epochs, args.data, args.weights, args.device)
    write_samples(args.data, args.samples, args.weights, args.device)


if __name__ == "__main__":
    main()
