from flask import Blueprint, request, jsonify, make_response
from database.db import DB_adapter
from authservice.authservice import checkHeaderToken

import json

api_docPage = Blueprint('docPage', __name__)

db_adapter = DB_adapter()

# int_service = IntService()


def getMainDocInfo():
    token_status, user_id = checkHeaderToken(request.headers)
    if not token_status:
        return jsonify({"message": "Ключ доступа отсутсвует или является недействительным"}), 403
    doc_id = request.args.get('doc_id', None)

    print(doc_id, flush=True)


    if doc_id != None:
        doc_info, disc_info = db_adapter.getDocInfo(doc_id)
        print(doc_info, flush=True)
        if not doc_info:
            return jsonify({'message' : 'doc not found'}), 404


        result = dict()
        result['id'] = doc_info['id']
        result['author'] = doc_info['author']
        result['title'] = doc_info['title']
        result['source'] = doc_info['source']
        result['doc_type'] = {"id": doc_info['type_doc_id'], "title": doc_info['doc_type_title']}
        if doc_info['theme_id'] != None:
            result['doc_theme'] = {"id": doc_info['theme_id'], "title": doc_info['theme_title']}
        
        if disc_info and len(disc_info):
            result['discs'] = list()
            for disc in disc_info:
                result['discs'].append({"id": disc['id'], "title": disc['title']})    
        
        return jsonify({'doc': result}), 200
    
    else:
        return jsonify({'message' : 'bad req'}), 400
    
    

def getDocContents():
    token_status, user_id = checkHeaderToken(request.headers)
    if not token_status:
        return jsonify({"message": "Ключ доступа отсутсвует или является недействительным"}), 403
    doc_id = request.args.get('doc_id', None)
    if doc_id != None:
        content = db_adapter.getDocContent(doc_id)

        if content and len(content):
            return jsonify({'contents': content}), 200

        else:
            return jsonify({'message' : 'no content'}), 205
    else:
        return jsonify({'message' : 'bad req'}), 400
    

def getDocComments():
    token_status, user_id = checkHeaderToken(request.headers)
    if not token_status:
        return jsonify({"message": "Ключ доступа отсутсвует или является недействительным"}), 403
    doc_id = request.args.get('doc_id', None)

    if doc_id != None:
        comments = db_adapter.getDocComments(doc_id)

        if comments and len(comments):
            return jsonify({'comments': comments}), 200

        else:
            return jsonify({'message' : 'no comments'}), 205
    else:
        return jsonify({'message' : 'bad req'}), 400
    
def getDocTermsShingles():
    token_status, user_id = checkHeaderToken(request.headers)
    if not token_status:
        return jsonify({"message": "Ключ доступа отсутсвует или является недействительным"}), 403
    doc_id = request.args.get('doc_id', None)

    if doc_id != None:
        terms_shingles = db_adapter.getDocTermsShingles(doc_id)
        if terms_shingles:
            terms, shingles = terms_shingles
            return jsonify({'terms': terms, 'shingles':shingles}), 200
        else:
            return jsonify({'message' : 'no terms && shingles'}), 205
    else:
        return jsonify({'message' : 'bad req'}), 400
    

def getDocDiscRelevance():
    token_status, user_id = checkHeaderToken(request.headers)
    if not token_status:
        return jsonify({"message": "Ключ доступа отсутсвует или является недействительным"}), 403
    doc_id = request.args.get('doc_id', None)

    if doc_id != None:
        disc = db_adapter.getDocDiscRelevance(doc_id)
        if disc:
            return jsonify({'discs': disc}), 200
        else:
            return jsonify({'message' : 'no rel disc'}), 205
    else:
        return jsonify({'message' : 'bad req'}), 400
    

def getMostRelevantDocs():
    token_status, user_id = checkHeaderToken(request.headers)
    if not token_status:
        return jsonify({"message": "Ключ доступа отсутсвует или является недействительным"}), 403
    doc_id = request.args.get('doc_id', None)

    if doc_id != None:
        docs_data = db_adapter.getMostRelevantDocs(doc_id)
        if not docs_data:
            return jsonify({'message' : 'docs not found'}), 205
        
        result = {'docs': list()}
        
        for doc in docs_data:
            doc_info = dict()
            doc_info['id'] = doc['id']
            doc_info['author'] = doc['author']
            doc_info['title'] = doc['title']
            doc_info['source'] = doc['source']
            doc_info['doc_type'] = {"id": doc['type_doc_id'], "title": doc['doc_type_title']}
            if doc['theme_id'] != None:
                doc_info['doc_theme'] = {"id": doc['theme_id'], "title": doc['theme_title']}

            result['docs'].append({"document": doc_info, "rel": doc['rel']})
        
        return jsonify(result), 200
    else:
        return jsonify({'message' : 'bad req'}), 400
    
    
        

api_docPage.add_url_rule('/doc_info', view_func=getMainDocInfo, methods=['GET'])
api_docPage.add_url_rule('/doc_content', view_func=getDocContents, methods=['GET'])
api_docPage.add_url_rule('/doc_disc', view_func=getDocDiscRelevance, methods=['GET'])
api_docPage.add_url_rule('/doc_terms_shingles', view_func=getDocTermsShingles, methods=['GET'])
api_docPage.add_url_rule('/doc_relevance', view_func=getMostRelevantDocs, methods=['GET'])
api_docPage.add_url_rule('/doc_comments', view_func=getDocComments, methods=['GET'])