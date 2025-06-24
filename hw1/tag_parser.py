from flask import Flask, request, jsonify
import yake

app = Flask(__name__)

@app.route('/keywords', methods=['POST'])
def keywords_endpoint():
    text = request.get_json().get('text', '')
    if not isinstance(text, str) or len(text.strip()) == 0:
        return jsonify({"error": "Invalid text input"}), 400
    
    try:
        custom_kw_extractor = yake.KeywordExtractor(
                lan="ru",
                n=2,
                dedupLim=0.5,
                windowsSize=1,
                top=10,
        )
        return jsonify({"keywords": [kw[0] for kw in custom_kw_extractor.extract_keywords(text)]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return "running"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)