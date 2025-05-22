from psycopg2 import connect, extras
from dotenv import load_dotenv
from os import getenv
import requests
import json

import pymorphy2
morph = pymorphy2.MorphAnalyzer()


class DB_adapter:

    def __init__(self):
        load_dotenv()
        self.db_name = getenv('DB_NAME')
        self.db_user = getenv('DB_USER')
        self.db_host = getenv('DB_HOST')
        self.db_port = getenv('DB_PORT')
        self.db_password = getenv('DB_PASSWORD')

        # self.makeConnect()

        # self.IntService = IntService()

    def _get_connection(self):
        conn = connect(
            database=self.db_name,
            user=self.db_user,
            host=self.db_host,
            port=self.db_port,
            password=self.db_password,
        )
        return conn, conn.cursor(cursor_factory=extras.RealDictCursor)

    # def makeConnect(self, ):
    #     conn = connect(
    #         database=self.db_name,
    #         user=self.db_user,
    #         host=self.db_host,
    #         port=self.db_port,
    #         password=self.db_password,
    #     )
    #     cur = conn.cursor(cursor_factory=extras.RealDictCursor)

    def getCollectionDisc(self):
        conn, cur = self._get_connection()
        cur.execute(
            "SELECT * FROM document WHERE id IN (SELECT doc_id FROM in_doc_collections WHERE coll_id = %s)", [12000])
        collections = cur.fetchall()
        cur.close()
        conn.close()

        return collections

    def getDocumentsFromUserQuery(self, user_query_string, collection, filter):
        conn, cur = self._get_connection()
        terms_negative_query = terms_positive_query = term_cond = base_filer_query = collection_query = user_query = ""
        collection_params, filter_params, user_params = dict(), dict(), dict()

        if filter:
            terms_positive_query, terms_negative_query, term_cond, base_filer_query, filter_params = self.makeQueryForFilter(
                filter)

        if collection:
            collection_query, collection_params = self.makeQueryForCollection(
                collection)

        if user_query_string:
            user_query, user_params = self.makeQueryForUserSearch(
                user_query_string)

        query = """SELECT document.id, 
            document.author AS author, 
            document.title AS title, 
            document.source_url AS source ,
            document.type_doc_id,
            document.publish_date,
			document.publisher,
            in_doc_type.title AS doc_type_title,
			in_doc_themes.theme_id,
			in_theme.title AS theme_title  """
        if user_query:
            query += """
                , array_position(%(user_query_ids)s, document.id) as rel
            """
        query += " FROM document "
        query += "LEFT JOIN in_doc_disciplines ON in_doc_disciplines.doc_id = document.id "
        query += "LEFT JOIN in_discipline ON in_doc_disciplines.discipline_id = in_discipline.id "
        query += """
            LEFT JOIN in_doc_type ON in_doc_type.id = document.type_doc_id
			LEFT JOIN in_doc_themes ON in_doc_themes.doc_id = document.id
			LEFT JOIN in_theme ON in_theme.id = in_doc_themes.theme_id
        """

        if terms_negative_query or base_filer_query or collection_query or user_query:
            querys_list = [terms_negative_query,
                           base_filer_query, collection_query, user_query]
        else:
            querys_list = []

        print(len(terms_positive_query), len(
            querys_list), len(term_cond), flush=True)
        if len(terms_positive_query) != 0:
            query += terms_positive_query

        query += " WHERE type_doc_id < 5"

        if len(querys_list):
            query += " AND " + \
                " AND ".join([item for item in querys_list if item])

        if len(term_cond):
            query += " " + term_cond

        if user_query and not len(term_cond):
            query += " ORDER BY rel ASC"
        if user_query and len(term_cond):
            query += "  , rel ASC"

        params = {**filter_params, **collection_params, **user_params}

        print(query, params, flush=True)
        cur.execute(query, params)

        res = cur.fetchall()
        cur.close()
        conn.close()
        return res

    def getDocumentIdsFromUserQuery(self, user_query_string):
        conn, cur = self._get_connection()
        new_words = user_query_string.split()
        stop_pos = {'PREP', 'CONJ', 'NPRO', 'PRCL', 'INTJ'}
        word_counts_separate = {}
        for j, word in enumerate(new_words):
            # Получаем первый вариант разбора слова
            parsed_word = morph.parse(word)[0]
            new_word = parsed_word.normal_form.upper()
            if parsed_word.tag.POS not in stop_pos:
                if new_word in word_counts_separate:
                    word_counts_separate[new_word] += 1
                else:
                    word_counts_separate[new_word] = 1

        query = f"WITH res AS (SELECT * FROM search_text(%s::text[], ARRAY[]::text[]) WHERE ip IN (SELECT doc_id FROM in_doc_collections) ORDER BY rel DESC) SELECT res.ip as id FROM res;"
        new_terms = list(word_counts_separate.keys())

        cur.execute(query, (new_terms, ))
        output_doc_ids = cur.fetchall()
        cur.close()
        conn.close()

        return output_doc_ids

    def makeQueryForUserSearch(self, user_query_string):
        query = ""
        query_params = dict()

        ids = self.getDocumentIdsFromUserQuery(user_query_string)
        if ids:
            query = "document.id = ANY(%(user_query_ids)s)"
            query_params['user_query_ids'] = [row['id'] for row in ids]

        return query, query_params

    def getDocumentsTerms(self, doc_ids):
        conn, cur = self._get_connection()
        if not doc_ids:
            return []

        query = """
            SELECT term, SUM(weight) AS w 
            FROM ip_term 
            WHERE weight > 0 AND ip_id = ANY(%s) 
            GROUP BY term 
            ORDER BY w DESC 
            LIMIT 10
        """
        params = (doc_ids,)

        cur.execute(query, params)

        res = cur.fetchall()
        cur.close()
        conn.close()

        return res

    def makeQueryForCollection(self, collection):
        query = ""
        query_params = dict()

        query = "document.id IN (SELECT ip FROM search_set(%(collection)s::integer[]))"
        query_params['collection'] = list(collection)

        return query, query_params

    def makeQueryForFilter(self, filter):
        term_positive, term_negative, term_cond, terms_params = self.makeQueryForTerms(
            filter.get('positive_terms', None), filter.get('negative_terms', None))
        base_filter_q, base_filter_params = self.makeQueryForBaseFilter(
            filter.get('types', None),
            filter.get('authors', None),
            filter.get('disciplines', None),
            filter.get('publishing_house', None),
            filter.get('year_start', None),
            filter.get('year_end', None),
        )
        filter_params = {**terms_params, **base_filter_params}

        return term_positive, term_negative, term_cond, base_filter_q, filter_params

    def makeQueryForTerms(self, positive_terms, negative_terms):
        query_positive = ""
        query_negative = ""
        positive_cond = ""
        query_params = dict()

        if positive_terms:
            query_positive = "JOIN (SELECT ip as term_ip, rel as term_rel FROM search_text(%(positive_terms)s::text[], ARRAY[]::text[])) p ON document.id = p.term_ip"
            positive_cond = "ORDER BY p.term_rel"
            query_params['positive_terms'] = list(
                [t.upper() for t in positive_terms])
        if negative_terms:
            query_negative = "NOT document.id = ANY(SELECT ip FROM search_text(%(negative_terms)s::text[], ARRAY[]::text[]))"
            query_params['negative_terms'] = list(
                [t.upper() for t in negative_terms])

        return query_positive, query_negative, positive_cond, query_params

    def makeQueryForBaseFilter(self, types, authors, disciplines, publishing_house, year_start, year_end):
        query_parts = []
        query_params = dict()
        if types:
            query_parts.append("document.type_doc_id = ANY(%(types)s)")
            query_params['types'] = list(types)

        if authors:
            query_parts.append("document.author LIKE ANY(%(authors)s)")
            query_params['authors'] = list(authors)

        if disciplines:
            query_parts.append(
                "in_doc_disciplines.discipline_id = ANY(%(disciplines)s)")
            query_params['disciplines'] = list(disciplines)

        if publishing_house:
            query_parts.append(
                "document.publisher LIKE ANY(%(publishing_house)s)")
            query_params['publishing_house'] = list(publishing_house)

        if year_start:
            query_parts.append("document.publish_date >= (%(year_start)s)")
            query_params['year_start'] = year_start

        if year_end:
            query_parts.append("document.publish_date <= (%(year_end)s)")
            query_params['year_end'] = year_end

        return " AND ".join(query_parts), query_params

    def getDocInfo(self, doc_id):
        conn, cur = self._get_connection()

        cur.execute("""
            SELECT document.id, 
            document.author AS author, 
            document.title AS title, 
            document.source_url AS source ,
            document.type_doc_id,
            in_doc_type.title AS doc_type_title,
			in_doc_themes.theme_id,
			in_theme.title AS theme_title
            FROM document 
            LEFT JOIN in_doc_type ON in_doc_type.id = document.type_doc_id
			LEFT JOIN in_doc_themes ON in_doc_themes.doc_id = document.id
			LEFT JOIN in_theme ON in_theme.id = in_doc_themes.theme_id
            WHERE document.id = %s
        """, [doc_id])


        print(doc_id, flush=True)

        main_info = cur.fetchone()

        cur.execute("""
            SELECT * FROM document 
            JOIN in_doc_disciplines ON in_doc_disciplines.doc_id = document.id
            JOIN in_discipline ON in_discipline.id = in_doc_disciplines.discipline_id
            WHERE document.id = %s
        """, [doc_id])

        disc_info = cur.fetchall()

        cur.close()
        conn.close()
        
        return main_info, disc_info

    def getDocContent(self, doc_id):
        conn, cur = self._get_connection()
        cur.execute("""
            SELECT * FROM public.in_doc_contents where doc_id = %s
            ORDER BY level, ordernum ASC 
        """, [doc_id])

        content = cur.fetchall()
        cur.close()
        conn.close()
        
        return self.getDocContentRec(content)

    def getDocContentRec(self, content):
        nodes = {row['id']: {
            'title': row['title'],
            'p_count': row['start_page'],
            'contents': []
        } for row in content}

        root_nodes = []

        for row in content:
            node = nodes[row['id']]
            parent_id = row['parent_unit']

            if parent_id is None:
                root_nodes.append(node)
            else:
                parent = nodes[parent_id]
                parent['contents'].append(node)

        return root_nodes

    def getDocComments(self, doc_id):
        conn, cur = self._get_connection()
        cur.execute("""
            SELECT * FROM in_comment WHERE doc_id = %s
        """, [doc_id])

        comments = cur.fetchall()

        result = {"comments": []}
        for c in comments:
            result['comments'].append(
                {"id": c['id'], "user_id": c['user_id'], "text": c['body'], "time": c['update_time']})

        cur.close()
        conn.close()

        return result

    def getDocTermsShingles(self, doc_id):
        conn, cur = self._get_connection()
        cur.execute("""
            select term, sum(weight) as rel from ip_term where weight > 0 and ip_id = %s group by term order by rel desc limit 10
        """, [doc_id])

        terms = cur.fetchall()

        cur.execute("""
            select term as shingle, sum(weight) as rel from ip_term_shingles where weight > 0 and ip_id = %s group by term order by rel desc limit 10
        """, [doc_id])

        shingles = cur.fetchall()
        cur.close()
        conn.close()

        if terms or shingles:
            result = terms, shingles
        else:
            result = False

        return result

    def getDocDiscRelevance(self, doc_id):
        conn, cur = self._get_connection()
        cur.execute("""
            select dst.ip_id as id, dst.rel, document.title from (select dst.ip_id, sum(src.weight + least(src.weight, dst.weight)) rel from ( select term, weight from ip_term 
                        where ip_id in(%s) and ip_id is not NULL and weight > 0) src 
                        inner join (select ip_id, term, weight from ip_term where ip_id in(select id from in_discipline) and weight > 0) dst 
                        on src.term = dst.term group by dst.ip_id order by rel desc limit 5) as dst
            inner join document on dst.ip_id = document.id
            order by rel desc
        """, [doc_id])

        disc = cur.fetchall()

        cur.close()
        conn.close()

        if disc:
            result = disc
        else:
            result = False

        return result

    def getMostRelevantDocs(self, doc_id):
        conn, cur = self._get_connection()
        cur.execute("""
            select sim.rel, d_info.*  from (
                select id1 as id, sum((sim+revsim) * psize.s) rel
                from in_portrait_sim psim 
                inner join (select ip_id id, sum(times) s from ip_term where weight > 0 and ip_id in (%s) group by ip_id) psize on psim.id2 = psize.id 
                where id2 in (%s) and id1 not in (%s) group by id1 order by rel desc limit 5
            ) as sim
            inner join (
                SELECT document.id, 
                document.author AS author, 
                document.title AS title, 
                document.source_url AS source ,
                document.type_doc_id,
                in_doc_type.title AS doc_type_title,
                in_doc_themes.theme_id,
                in_theme.title AS theme_title
                FROM document 
                LEFT JOIN in_doc_type ON in_doc_type.id = document.type_doc_id
                LEFT JOIN in_doc_themes ON in_doc_themes.doc_id = document.id
                LEFT JOIN in_theme ON in_theme.id = in_doc_themes.theme_id
            ) as d_info on d_info.id = sim.id
            order by rel desc
        """, [doc_id, doc_id, doc_id])

        docs = cur.fetchall()
        cur.close()
        conn.close()

        return docs

    def getUserWorkshops(self, user_id):
        if not user_id:
            return False
        conn, cur = self._get_connection()

        cur.execute("""
            SELECT id, title FROM workshop WHERE user_id = %s
        """, [user_id])

        user_workshops = cur.fetchall()
        cur.close()
        conn.close()

        return user_workshops

    def createWorkshop(self, user_id, title):
        if not user_id or not title:
            return False
        conn, cur = self._get_connection()

        cur.execute("""
            INSERT INTO workshop (user_id, title) VALUES (%s, %s) RETURNING id
        """, [user_id, title])

        workshop_id = cur.fetchone()['id']

        conn.commit()
        cur.close()
        conn.close()

        return workshop_id

    def editWorkshopInfo(self, user_id, workshop_id, title):
        if not user_id or not title or not workshop_id:
            return False
        conn, cur = self._get_connection()

        cur.execute("""
            UPDATE workshop SET title = %s WHERE user_id = %s AND id = %s RETURNING id
        """, [title, user_id, workshop_id])

        edit_id = cur.fetchone()
        if not edit_id:
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True

    def delWorkshop(self, user_id, workshop_id):
        if not user_id or not workshop_id:
            return False
        conn, cur = self._get_connection()

        cur.execute("""
            DELETE FROM workshop WHERE user_id = %s AND id = %s RETURNING id
        """, [user_id, workshop_id])

        del_id = cur.fetchone()
        if not del_id:
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True


    def getUserBlocks(self, user_id, workshop_id):
        if not user_id or not workshop_id:
            return False
    
        conn, cur = self._get_connection()
        cur.execute("""
            SELECT id as block_id, content as block_content FROM workshop_block WHERE workshop_id in (SELECT id FROM workshop WHERE user_id = %s AND id = %s)
        """, [user_id, workshop_id])


        user_workshop_blocks = cur.fetchall()
        cur.close()
        conn.close()

        return user_workshop_blocks
    
    def addUserBlock(self, user_id, workshop_id):
        if not user_id or not workshop_id:
            return False
        conn, cur = self._get_connection()
        cur.execute("""
            SELECT id FROM workshop WHERE user_id = %s AND id = %s
        """,[user_id, workshop_id])    
        user_to_workshop = cur.fetchone()
        if not user_to_workshop:
            return False

        cur.execute("""
            INSERT INTO workshop_block (workshop_id) VALUES (%s) RETURNING id
        """,[workshop_id])

        block_id = cur.fetchone()['id']

        conn.commit()
        cur.close()
        conn.close()

        return block_id
    

    def changeUserBlockContent(self, user_id, workshop_id, block_id, content):
        if not user_id or not workshop_id or not block_id or content is None:
            return False
        conn, cur = self._get_connection()
        cur.execute("""
            SELECT id FROM workshop WHERE user_id = %s AND id = %s
        """,[user_id, workshop_id])    
        user_to_workshop = cur.fetchone()
        if not user_to_workshop:
            return False
        
        cur.execute("""
            UPDATE workshop_block SET content = %s WHERE workshop_id = %s AND id = %s RETURNING id
        """,[content, workshop_id, block_id])    
        
        edit_id = cur.fetchone()

        if not edit_id:
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True
    
    def delUserBlock(self, user_id, workshop_id, block_id):
        if not user_id or not workshop_id or not block_id:
            return False
        conn, cur = self._get_connection()

        cur.execute("""
            SELECT id FROM workshop WHERE user_id = %s AND id = %s
        """,[user_id, workshop_id])    
        user_to_workshop = cur.fetchone()
        if not user_to_workshop:
            return False


        cur.execute("""
            DELETE FROM workshop_block WHERE workshop_id = %s AND id = %s RETURNING id
        """,[workshop_id, block_id])    
        
        del_id = cur.fetchone()

        if not del_id:
            return False

        conn.commit()
        cur.close()
        conn.close()

        return True