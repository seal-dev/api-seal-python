from flask import Flask, request, jsonify, make_response
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from db import Querys
import json
import jwt
import datetime
import psycopg2 as db
import logging

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
            'id_localabastecimento': i[0],
            'descricao': i[1],
            'ativo': i[2]
        }
        
        lista.append(retorno)    
   
    return jsonify(lista)

@app.route('/v1/local/get/<string:matriz>', methods=['GET'])
@jwt_required()
def local(matriz):
    test_client = app.test_client()
    resposta = test_client.post()

    fields = ['*']
    
    select = query.select('app_funcionario', ', '.join(fields), 'and' , f"empresa_id = {int(matriz)}")
                    
    response = query.fecthall()

    lista = []

    for i in response:

        id = i[6]

        select = query.select('app_localabastecimento', ', '.join(fields), 'and' , f"id = {id}")

        try:
            response_local = query.fecthall()
                                                                                                                                                                                                                                
            for j in response_local:
                local_op = j[1]
        except db.ProgrammingError:
            local_op = None

        retorno = {
            'id': id,
            'id_operadores': i[4],
            'local': local_op
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

@app.route('/v1/bicos/bicoscomboio/<string:idFilial>/<string:nroBico>', methods=['GET'])
@jwt_required()
def bicos(idFilial, nroBico):
    fields = ['*']
    
    select = query.select('app_bico', ', '.join(fields), 'and', f"empresa_id={int(idFilial)}", f"codigo_bico={int(nroBico)}")

    response = query.fecthall()

    lista = []
    
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
        ccs_id = i[9]
        empresa_id = i[10]
        local_abast_id = i[11]
        tanque_id = i[12]
        status = i[13]
        codigo_bico = i[14]

        select_tanques = query.select('app_tanques', ', '.join(fields), 'and', f"id = {tanque_id}")

        response_tanque = query.fecthall()

        for j in response_tanque:
            id_produto = j[9]
            
            select_produto = query.select('app_produtos', ', '.join(fields), 'and', f"id = {id_produto}")

            response_produto = query.fecthall()

            for k in response_produto:
                descricao_produto = k[2]

                retorno = {
                    "id_bico": codigo_bico,
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

@app.route('/v1/bicos/get/<string:bicoid>', methods=['GET'])
@jwt_required()
def bicos_2(bicoid):
    fields = ['*']

    select = query.select('app_bico', ', '.join(fields), 'and', f"id = {bicoid}")

    response = query.fecthall()

    lista = []
    
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
        nivel = i[1]
        
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

@app.route('/v1/operadores/get/<string:idMatriz>', methods=['GET'])
@jwt_required()
def operadores(idMatriz):

    fields = ['*']

    select = query.select('app_funcionario', ', '.join(fields), 'and' , f"empresa_id = {idMatriz}")
                    
    response = query.fecthall()

    lista = []

    for i in response:
        id_funcionario = i[0]
        id_usuario = i[4]
        senha_app = i[2]           
        nivel = i[1]
        login = i[5]

        retorno = {
                "id_usuario": id_usuario,
                "senha": senha_app,
                "login": login,
                "tipo": nivel
            }

        if nivel >= 1:
            lista.append(retorno)

    return jsonify(lista)

@app.route('/v1/config/get/<string:idMatriz>', methods=['GET'])
@jwt_required()
def get_configcomboio(idMatriz):
    fields = ['*']

    select = query.select('app_config', ', '.join(fields), 'and' , f"empresa_id = {idMatriz}")
                    
    response = query.fecthall()

    lista = []

    login = 0
    senha = 0
    for i in response:
        if i[1] is True:
            login = 1
        if i[2] is True:
            senha = 1

        retorno = {
                'login' : login,
                'senha' : senha,
                'preodo' : 1,
                'bloqkmhr' : 1
            }

        lista.append(retorno)

    return jsonify(lista)

@app.route('/v1/abastecimentos/get/<string:idFilial>', methods=['GET'])
@jwt_required()
def get_abastecimentos(idFilial):

    fields = ['*']
    
    select = query.select('app_abastecimento', ', '.join(fields), 'and' , f"empresa_filial_id = {int(idFilial)}")
    
    try:
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
        
       
        return jsonify(lista), 200

    except Exception as e:

        return str(e)
   

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
        
        if i[11] > 0:
            consumo = 1
        else:
            consumo = 0

        nropulsos = 0
        if nropulsos is not None:
            nropulsos = i[14]
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
                "ctrlconsumo": consumo,
                "id_checklist": i[8],
                "exibemedia": 0,
                "ctrlhrkm": i[11],
                "codteclado": f"{i[2]}".upper(),
                "ativo": i[12],
                "pulsoskm": i[13],
                "nropulsos": float(i[14]),
                "iptmct": '{}'.format(i[3]),
                "kmbase": i[15],
                "hrbase": i[16]
        }
        lista.append(retorno)
    
    return jsonify(lista)

@app.route('/v1/abastecimento/salvar/<string:idFilial>/<string:nroBico>', methods=['POST'])
@jwt_required()
def abastecimento(idFilial, nroBico):
    body = request.get_json()
    print(body)
    select_bico = query.select('app_bico', 'id', 'and', f"empresa_id={int(idFilial)}", f"codigo_bico={int(nroBico)}")

    try:
        response_bico = query.fecthall()
    

        for i in response_bico:
            id_bico = i[0]
        print(id_bico)
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
                        "telemetria",
                        "abast_manual"]

        error = False
        
        for i, data in enumerate(lista_fields):
            if lista_fields[i] not in body:
                error = True
                return {"message": f"está faltando o campo '{lista_fields[i]}', que é um campo obigatório."}
        print(error)
        if error is not True:
            
            nomes_campos_dict = body.keys()
            valores_campos_dict = body.values()

            nomes_campos_db = []
            valores_campos = []

            for i in nomes_campos_dict:
                
                if i == 'idfilial':
                    i = 'empresa_filial_id'

                if i == 'idcomboio':
                    i = 'id_comboio_id'

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

            for i in valores_campos_dict:
                valores_campos.append(i)

            print(nomes_campos_db)
            print(len(valores_campos), len(nomes_campos_db))
            for i, data in enumerate(nomes_campos_db):
                
                if data == 'bico_id':
                    print(i)
                    valores_campos[i] = id_bico

            logging.warning(valores_campos)
            insert = query.insert('app_abastecimento', ', '.join(nomes_campos_db), tuple(valores_campos))
            return {'success' : 'inserido com sucesso'}
    except db.ProgrammingError:

        return {'error' : 'abastecimento já realizado'}

@app.errorhandler(500)
def internal_error(error):
    return "500 error"

@app.errorhandler(404)
def not_found(error):
    return "404 error", 404

if __name__ == '__main__':
    app.run(host='45.15.24.171', port='5000', debug=True)