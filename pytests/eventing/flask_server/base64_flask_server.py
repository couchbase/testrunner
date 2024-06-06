from flask import Flask, request, jsonify
from flask_cors import CORS
from sentence_transformers import SentenceTransformer

app = Flask(__name__)
CORS(app)
model = SentenceTransformer('sentence-transformers/paraphrase-MiniLM-L6-v2')

@app.route('/encode', methods=['POST'])
def encode_text():
    data = request.json
    if 'texts' in data:
        texts = str(data['texts'])
        encodings = model.encode(texts).tolist()
        return jsonify(encodings)
    else:
        return ""

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
