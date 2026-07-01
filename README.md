# Apollo - This application has been made 95% by AI and 5% by me

Apollo is a gRPC service that runs machine‑learning **classification models over images and video**. You submit an input (an image or video URL, or a stream of raw bytes) together with the names of the models to run on it; Apollo returns a **task id** immediately and does the work asynchronously. You then poll for the result or receive it over a webhook.

Models are loaded from [Hugging Face](https://huggingface.co/) and executed with the [candle](https://github.com/huggingface/candle) inference framework. The service is built to be operated as a long‑running daemon: task state is fully persisted, so work survives restarts, and interrupted video scans resume from where they stopped.

---

Reason AI was used is because I needed a quick solution for classifying images and videos with options to expand to audio and text. 
Using AI made it so that I could focus on other tasks. But do not be deluded this is probably not a perfect application. It probably has obvious bugs which I won't be bothered to search for.

Feel free to send pull requests. Do not care if it's AI because this is made with AI but at the very least test it first.

---

See [docs](./docs/README.md) for more documentation 