import pandas as pd
from pymongo import MongoClient

# DataFrame para as informações do carro
carros = pd.DataFrame({
    'Carro': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
    'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']
})

# DataFrame para as informações da montadora
montadoras = pd.DataFrame({
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda'],
    'País': ['EUA', 'Alemanhã', 'França', 'EUA', 'Japão'],
})

# Criar uma conexão com o MongoDB
client = MongoClient('mongodb://localhost:27017/')
database = client['DataOps_Validation'] 
carros_collection = database['Carros']
montadoras_collection = database['Montadoras']

# Converter os DataFrames em formato de dicionário
carros_dict = carros.to_dict('records')
montadoras_dict = montadoras.to_dict('records')

# Inserir os documentos nas coleções correspondentes
carros_collection.insert_many(carros_dict)
montadoras_collection.insert_many(montadoras_dict)

# Realizar a operação de junção (join) usando agregação e agrupar por país
pipeline = [
    {
        '$lookup': {
            'from': 'Montadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'Montadora_info'
        }
    },
    {
        '$group': {
            '_id': '$Montadora_info.País',
            'Carros': {
                '$push': {
                    'Modelo': '$Modelo',
                    'Ano': '$Ano',
                    'Cor': '$Cor'
                }
            }
        }
    }
]

result = list(carros_collection.aggregate(pipeline))

# Fechar a conexão com o MongoDB
client.close()
