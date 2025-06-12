from google import genai

from news_letter_maker.utils.env import GEMINI_API_KEY


def generate(prompt: str) -> str:

    client = genai.Client(
        api_key=GEMINI_API_KEY,
    )

    response = client.models.generate_content(
        model="gemini-2.0-flash-lite",
        contents=prompt,
    )

    return response.text
