import os
import json
import requests
import sqlite3
import duckdb
import subprocess
import shutil
import markdown
import csv
from fastapi import FastAPI, HTTPException
from pathlib import Path
from PIL import Image
from pydub import AudioSegment
import speech_recognition as sr
from git import Repo

app = FastAPI()

DATA_DIR = Path("/data")

def ensure_data_path(filepath):
    """Ensure the file path is within /data."""
    path = Path(filepath).resolve()
    if not path.is_relative_to(DATA_DIR):
        raise HTTPException(status_code=403, detail="Access outside /data is forbidden")
    return path

def B3(api_url: str, filename: str):
    """Fetch data from an API and save it."""
    response = requests.get(api_url)
    response.raise_for_status()
    save_path = ensure_data_path(filename)
    with open(save_path, "w", encoding="utf-8") as file:
        json.dump(response.json(), file, indent=4)

def B4(repo_url: str, commit_message: str, file_to_change: str, new_content: str):
    """Clone a git repo, modify a file, and commit the change."""
    repo_path = ensure_data_path("/data/repo")
    if repo_path.exists():
        shutil.rmtree(repo_path)
    Repo.clone_from(repo_url, repo_path)
    file_path = repo_path / file_to_change
    ensure_data_path(file_path)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    repo = Repo(repo_path)
    repo.git.add(file_path)
    repo.index.commit(commit_message)
    repo.remote().push()

def B5(db_path: str, query: str, output_file: str, use_duckdb: bool = False):
    """Run a SQL query on SQLite or DuckDB."""
    db_path = ensure_data_path(db_path)
    output_file = ensure_data_path(output_file)
    conn = duckdb.connect(str(db_path)) if use_duckdb else sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f)

def B6(url: str, output_file: str):
    """Scrape a website and save data."""
    response = requests.get(url)
    response.raise_for_status()
    output_path = ensure_data_path(output_file)
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(response.text)

def B7(image_path: str, output_path: str, size=(100, 100)):
    """Compress or resize an image."""
    image_path = ensure_data_path(image_path)
    output_path = ensure_data_path(output_path)
    image = Image.open(image_path)
    image.thumbnail(size)
    image.save(output_path)

def B8(audio_path: str, output_file: str):
    """Transcribe audio from an MP3 file."""
    audio_path = ensure_data_path(audio_path)
    output_file = ensure_data_path(output_file)
    audio = AudioSegment.from_mp3(audio_path)
    wav_path = str(audio_path).replace(".mp3", ".wav")
    audio.export(wav_path, format="wav")
    recognizer = sr.Recognizer()
    with sr.AudioFile(wav_path) as source:
        audio_data = recognizer.record(source)
    text = recognizer.recognize_google(audio_data)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(text)
    os.remove(wav_path)

def B9(markdown_file: str, output_file: str):
    """Convert Markdown to HTML."""
    markdown_file = ensure_data_path(markdown_file)
    output_file = ensure_data_path(output_file)
    with open(markdown_file, "r", encoding="utf-8") as md:
        html = markdown.markdown(md.read())
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

@app.get("/filter_csv")
def B10(csv_file: str, column: str, value: str):
    """Filter a CSV file and return JSON."""
    csv_file = ensure_data_path(csv_file)
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        results = [row for row in reader if row.get(column) == value]
    return results
