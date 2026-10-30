import imagehash
from PIL import Image

def generate_phash(image: Image.Image) -> str:
    """
    Takes a PIL Image object and returns a 16-character perceptual hash.
    Separated so it can be imported by both the DB sync and the camera script.
    """
    # pHash (perceptual hash) looks at the frequency domain of the image,
    # making it highly resistant to color shifts and slight camera blurring.
    return str(imagehash.phash(image))
