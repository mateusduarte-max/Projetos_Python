import flask
from flask import request, jsonify

app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Create some test data for our catalog in the form of a list of dictionaries.
books = [
    {'id': 0,
     'title': 'A Fire Upon the Deep',
     'author': 'Vernor Vinge',
     'first_sentence': 'The coldsleep itself was dreamless.',
     'year_published': '1992'},
    {'id': 1,
     'title': 'The Ones Who Walk Away From Omelas',
     'author': 'Ursula K. Le Guin',
     'first_sentence': 'With a clamor of bells that set the swallows soaring, the Festival of Summer came to the city Omelas, bright-towered by the sea.',
     'published': '1973'},
    {'id': 2,
     'title': 'Dhalgren',
     'author': 'Samuel R. Delany',
     'first_sentence': 'to wound the autumnal city.',
     'published': '1975'}
]


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Distant Reading Archive</h1>
<p>A prototype API for distant reading of science fiction novels.</p>'''


@app.route('/api/v1/resources/books/all', methods=['GET'])
def api_all():
    return jsonify(books)


@app.route('/api/v1/resources/books', methods=['GET'])
def api_id():
    # Verifique se um ID foi fornecido como parte do URL.
    # Se o ID for fornecido, atribua-o a uma variável.
    # Se nenhum ID for fornecido, exibe um erro no navegador.
    if 'id' in request.args:
        id = int(request.args['id'])
    else:
        return "Error: No id field provided. Please specify an id."

    # Crie uma lista vazia para nossos resultados
    results = []

    # Faça um loop pelos dados e combine os resultados que se encaixam no ID solicitado.
    # IDs são únicos, mas outros campos podem retornar muitos resultados
    for book in books:
        if book['id'] == id:
            results.append(book)

    # Use a função jsonify do Flask para converter nossa lista de
    # Dicionários Python para o formato JSON.
    return jsonify(results)

app.run()