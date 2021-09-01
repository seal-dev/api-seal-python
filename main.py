from flask import Flask, request, jsonify, make_response
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from db import Querys
import json
import jwt
import datetime

app = Flask(__name__)

app.config['SECRET_KEY'] = 'secret'
app.config['JWT_IDENTITY_CLAIM'] = 'sub'

JWTManager(app)
query = Querys()

@app.route('/')
def home():
    return {'message': 'Preciso fornecer urls validas para a api.'}

@app.route('/v1/auth/refreshtoken/<string:device>', methods=['GET'])
def login(device):
    delta = datetime.timedelta(minutes=30)
    token = create_access_token(identity=device, expires_delta=delta)
    return json.dumps({'token': token})
 
@app.route('/v1/localabast/get/<string:matriz>', methods=['GET'])
@jwt_required()
def localabast(matriz):
    test_client = app.test_client()
    resposta = test_client.post()

    fields = ['*']
    
    select = query.select('app_localabastecimento', ', '.join(fields), 'and' , f"empresa_id = {int(matriz)}")
                    
    response = query.fecthall()

    lista = []

    for i in response:
        
        retorno = {
            'id': i[0],
            'descricao': i[1],
            'ativo': i[2]
        }
        
        lista.append(retorno)    

    return jsonify(lista)

@app.route('/v1/tanques/get/<string:idMatriz>', methods=['GET'])
@jwt_required()
def tanques(idMatriz):  
    
    fields = ['*']

    select = query.select('app_tanqueveiculo', ', '.join(fields), 'and' , f"empresa_id = {int(idMatriz)}")

    response = query.fecthall()

    lista = []

    for i in response:
        
        retorno = {
            "id_tanquesveiculos": i[0],
            "id_veiculos": i[7],
            "id_produtos1": i[4],
            "id_produtos2": i[5],
            "cod_xpid": '0',
            "tipo": 0,
            "ativo": i[3]
        }
        lista.append(retorno)

    
    return jsonify(lista)

@app.route('/v1/bicos/bicoscomboio/<string:filial>/<string:bico>', methods=['GET'])
@jwt_required()
def bicos(filial, bico):
    fields = ['*']

    select = query.select('app_bico', ', '.join(fields), 'and', f"empresa_id = {int(filial)}", f"id={int(bico)}")

    response = query.fecthall()

    lista = []
    print(response)
    for i in response:

        id_bico = i[0]
        leitura_atual = i[1]
        ppl = i[2]
        ip_bws = i[3]
        id_pulser = i[4]
        lado_pulser = i[5]
        porta_bico = i[6]
        mangueira = i[7]
        porta_eletrovalvula = i[8]
        nro_bico = i[9]
        ccs_id = i[10]
        empresa_id = i[11]
        local_abast_id = i[12]
        tanque_id = i[13]
        status = i[14]

        select_tanques = query.select('app_tanques', ', '.join(fields), 'and', f"id = {tanque_id}")

        response_tanque = query.fecthall()

        for j in response_tanque:
            id_produto = j[9]

            select_produto = query.select('app_produtos', ', '.join(fields), 'and', f"id = {id_produto}")

            response_produto = query.fecthall()

            for k in response_produto:
                descricao_produto = k[2]

                retorno = {
                    "id_bico": id_bico,
                    "id_produto": id_produto,
                    "nomeproduto": descricao_produto,
                    "vlrunit": 0,
                    "ip": ip_bws,
                    "porta": porta_bico,
                    "ladopulser": lado_pulser,
                    "idpulser": id_pulser,
                    "portaeletvalv": porta_eletrovalvula,
                    "idlocal": local_abast_id,
                }
    
                lista.append(retorno)

    return json.dumps(lista).encode('utf8')

@app.route('/v1/bicos/get/<string:idCcs>', methods=['GET'])
@jwt_required()
def bicos_2(idCcs):
    fields = ['*']

    select = query.select('app_bico', ', '.join(fields), 'and', f"ccs_id = {idCcs}")

    response = query.fecthall()

    lista = []
    print(response)
    for i in response:

        id_bico = i[0]
        leitura_atual = i[1]
        ppl = i[2]
        ip_bws = i[3]
        id_pulser = i[4]
        lado_pulser = i[5]
        porta_bico = i[6]
        mangueira = i[7]
        porta_eletrovalvula = i[8]
        nro_bico = i[9]
        ccs_id = i[10]
        empresa_id = i[11]
        local_abast_id = i[12]
        tanque_id = i[13]
        status = i[14]

        select_tanques = query.select('app_tanques', ', '.join(fields), 'and', f"id = {tanque_id}")

        response_tanque = query.fecthall()

        for j in response_tanque:
            id_produto = j[9]

            select_produto = query.select('app_produtos', ', '.join(fields), 'and', f"id = {id_produto}")

            response_produto = query.fecthall()

            for k in response_produto:
                descricao_produto = k[2]

                retorno = {
                    "id_bico": id_bico,
                    "id_produto": id_produto,
                    "nomeproduto": descricao_produto,
                    "vlrunit": 0,
                    "ip": ip_bws,
                    "porta": porta_bico,
                    "ladopulser": lado_pulser,
                    "idpulser": id_pulser,
                    "portaeletvalv": porta_eletrovalvula,
                    "idlocal": local_abast_id,
                }
    
                lista.append(retorno)

    return jsonify(lista)

@app.route('/v1/filial/get/<string:idfilial>', methods=['GET'])
@jwt_required()
def filial(idfilial):

    fields = ['*']

    select = query.select('app_empresafilial', ', '.join(fields), 'and' , f"id = {idfilial}")
                    
    response = query.fecthall()

    lista = []

    for i in response:
        
        retorno = {
            "id": i[13],
            "id_filial": i[0],
            "razaosocialfilial": i[1],
            "endereco": i[5],
            "cidade": i[9],
            "uf": i[10],
            "cnpj": i[3],
            "ie": i[4],
            "idcomboio": 0,
            "idcompserie": 0,
            "nrocompatual": 0
        }
        
        lista.append(retorno)    
    return jsonify(lista)

@app.route('/v1/funcionarios/get/<string:idMatriz>', methods=['GET'])
@jwt_required()
def funcionario(idMatriz):

    fields = ['*']

    select = query.select('app_funcionario', ', '.join(fields), 'and' , f"empresa_id = {idMatriz}")
                    
    response = query.fecthall()

    lista = []

    for i in response:
        id_funcionario = i[0]
        id_usuario = i[4]
        senha_app = i[2]           

        select_user = query.select('auth_user', 'first_name, is_active', 'and', f"id = {id_usuario}")
        
        response_user = query.fecthall()

        for j in response_user: 
            
            ativo = 0

            if j[1] is True:
                ativo = 1

            retorno = {
                    "id_funcionarios": id_funcionario,
                    "id_usuario": id_usuario,
                    "senha": senha_app,
                    "nome": j[0],
                    "ativo": ativo
                }
            
            lista.append(retorno)

    return jsonify(lista)

@app.route('/v1/abastecimentos/get/<string:idFilial>', methods=['GET'])
@jwt_required()
def get_abastecimentos(idFilial):

    fields = ['*']
    
    select = query.select('app_abastecimento', ', '.join(fields), 'and' , f"empresa_filial_id = {int(idFilial)}")
                    
    response = query.fecthall()

    lista = []

    for i in response:
        
        retorno = {
            "id": i[0],
            "idfilial": i[12],
            "idcomboio": i[1],
            "idbico": i[11],
            "data": f"{i[3]}",
            "qtde": i[4],
            "idplaca": i[14],
            "idfuncionario": i[16],
            "idoperador": i[15],
            "semtag": i[5],
            "odometro": float(i[6]),
            "horimetro": float(i[7]),
            "tag": f"{i[8]}",
            "local": i[13],
            "tipotq": i[17],
            "tipolib": i[18],
            "telemetria": i[10]
        }
        
        lista.append(retorno)   

    return jsonify(lista)


@app.route('/v1/placas/get/<string:idMatriz>', methods=['GET'])
@jwt_required()
def placas(idMatriz):

    fields = ['*']

    select = query.select('app_veiculos', ', '.join(fields), 'and', f"empresa_id = {int(idMatriz)} ORDER BY id;")

    response = query.fecthall()
    
    lista = []
    
    for i in response:

        select_modelo = query.select('app_modelo', ', '.join(fields), 'and', f"id = {i[22]} ")

        response_modelo = query.fecthall()

        for j in response_modelo:
            modelo = j[1]


        retorno = {
                "id_placas": i[0],
                "nroplaca": i[1],
                "ultimoodometro": '0',
                "tag": "string",
                "veiculo": modelo,
                "motorista": '{}'.format(0),
                "id_entidade": 0,
                "horimetro": '{}'.format(i[6]),
                "emitecompnf": i[10],
                "tpliberacao": i[9],
                "ctrlconsumo": i[11],
                "id_checklist": i[8],
                "exibemedia": 0,
                "ctrlhrkm": 0,
                "codteclado": "string",
                "ativo": i[12],
                "pulsoskm": i[13],
                "nropulsos": int(i[14]),
                "iptmct": "string",
                "kmbase": i[15],
                "hrbase": i[16]
        }
        lista.append(retorno)
    
    return jsonify(lista)

@app.route('/v1/abastecimento/salvar', methods=['POST'])
@jwt_required()
def abastecimento():

    body = request.get_json()
    
    lista_fields = ["id", 
                    "idfilial",
                    "idcomboio",
                    "idbico",
                    "data",
                    "qtde",
                    "idplaca",
                    "idfuncionario",
                    "idoperador",
                    "semtag",
                    "odometro",
                    "horimetro",
                    "tag",
                    "local",
                    "tipotq",
                    "tipolib",
                    "telemetria"]

    error = False

    for i, data in enumerate(lista_fields):
        if lista_fields[i] not in body:
            error = True
            return {"message": f"está faltando o campo '{lista_fields[i]}', que é um campo obigatório."}

    if error is not True:
    
        nomes_campos_dict = []
        valores_campos = []

        for i in body:

            nomes_campos_dict.append(i)
            valores_campos.append(body[i])

        nomes_campos_db = []

        for i in nomes_campos_dict:
            
            if i == 'idfilial':
                i = 'empresa_filial_id'

            if i == 'idcomboio':
                i = 'id_comboio'

            if i == 'idbico':
                i = 'bico_id'

            if i == 'data':
                i = 'data_abastecimento'

            if i == 'qtde':
                i = 'qtde_abastecida'

            if i == 'idplaca':
                i = 'veiculo_id'

            if i == 'idfuncionario':
                i = 'abastecedor_id'
                
            if i == 'idoperador':
                i = 'motorista_id'

            if i == 'semtag':
                i = 'sem_tag'

            if i == 'odometro':
                i = 'hodometro'

            if i == 'local':
                i = 'local_abastecimento_id'

            nomes_campos_db.append(i)
        
        insert = query.insert('app_abastecimento', ', '.join(nomes_campos_db), tuple(valores_campos))
        
        return insert


if __name__ == '__main__':
    app.run(host='192.168.1.58', port='7676', debug=True)