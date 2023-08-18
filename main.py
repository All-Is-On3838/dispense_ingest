from flask import Flask, request, abort
import json
import subprocess
import os

app = Flask(__name__)

has_run = False

@app.route('/', methods=['POST'])
def my_function():
    global has_run
    if not has_run:
        try:
            envelope = json.loads(request.data.decode('utf-8'))
            payload = envelope['message']['data']

            subprocess.run(['/usr/app/run.sh'])

            return ('', 204)
        except Exception as e:
            # Log the error
            print(e)
            # Return an error response
            abort(500)
        has_run = True
    return 'Already Started'

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))