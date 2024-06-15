from flask import Flask, request, jsonify, render_template
import threading
import os


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/adicionar_cep', methods=['POST'])
def adicionar_cep():
    data = request.get_json()
    cep = data.get('cep')
    if not cep:
        return jsonify({'error': 'CEP é obrigatorio!'}), 400
    
    # Remover o "-" do CEP
    cep = cep.replace('-', '')
    
    with open('CEPs.txt', 'a') as file:
        file.write(f'{cep}\n')
        
    
    thread = threading.Thread(target=run_worker)
    thread.start()
    
    return jsonify({'message': f'O cep {cep} foi inserido com sucesso.'})

def run_worker():
    os.system('python worker.py')


if __name__ == "__main__":
    app.run(debug=True)