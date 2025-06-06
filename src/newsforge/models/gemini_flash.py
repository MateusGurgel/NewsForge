import base64
import os
import json
from google import genai
from google.genai import types

from newsforge.env import GEMINI_API_KEY


class GeminiFlash:
    def __init__(self):
        self.client = genai.Client(
            api_key=GEMINI_API_KEY,
        )
        self.model = "gemini-2.0-flash-lite"

    def handle(self, system_prompt: str, input_text: str) -> dict[str,str]:
        contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=system_prompt),
                ],
            ),
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=input_text),
                ],
            ),
        ]
        generate_content_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=genai.types.Schema(
                type=genai.types.Type.OBJECT,
                required=["title", "news_origin", "resume", "transcription"],
                properties={
                    "title": genai.types.Schema(
                        type=genai.types.Type.STRING,
                    ),
                    "news_origin": genai.types.Schema(
                        type=genai.types.Type.STRING,
                    ),
                    "resume": genai.types.Schema(
                        type=genai.types.Type.STRING,
                    ),
                    "transcription": genai.types.Schema(
                        type=genai.types.Type.STRING,
                    ),
                },
            ),
        )

        response_text = ""
        for chunk in self.client.models.generate_content_stream(
            model=self.model,
            contents=contents,
            config=generate_content_config,
        ):
            response_text += chunk.text

        return json.loads(response_text)