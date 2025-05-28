"""
gsi_utils.py: This file contains methods for gsi index creation, drop, build and other utils

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 04/10/22 11:53 am

"""
import datetime
import random
import string
import time
import uuid
from threading import Event

import logger
from functools import reduce
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.query_definitions import QueryDefinition, RANGE_SCAN_TEMPLATE, RANGE_SCAN_ORDER_BY_TEMPLATE, \
    FULL_SCAN_ORDER_BY_TEMPLATE


class GSIUtils(object):
    def __init__(self, query_obj, encoder=None):
        self.initial_index_num = 0
        self.log = logger.Logger.get_logger()
        self.definition_list = []
        self.run_query = query_obj
        self.batch_size = 0
        self.query_event = Event()
        self.encoder = encoder

    def set_encoder(self, encoder):
        self.encoder = encoder

    def generate_mini_car_vector_index_definition(self, index_name_prefix=None,
                                                  skip_primary=False, array_indexes=True):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "docloader" + str(uuid.uuid4()).replace("-", "")

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=5))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "rating >= 4"),
                                is_primary=True))

        # Single vector field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'descriptionVector',
                            index_fields=['descriptionVector VECTOR'],
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format("*", 'APPROX_L2_DIST '
                                                                                   f'descriptionVector vectorValue')))

        # Single vector + single scalar field + partitioned on vector
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'oneScalarLeadingOneVector',
                            index_fields=['description', 'descriptionVector VECTOR'],
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                               'description like "%%Audi%%"',
                                                                               'APPROX_L2_DIST '
                                                                               f'descriptionVector vectorValue'),
                            partition_by_fields=['descriptionVector']))

        # Single vector (leading) + single scalar field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'oneScalarOneVectorLeading',
                            index_fields=['descriptionVector VECTOR', 'id'],
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                               'id like "%%58%%"',
                                                                               'APPROX_L2_DIST '
                                                                               f'descriptionVector vectorValue')))

        # Single vector + multiple scalar field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'multiScalarOneVector_1',
                            index_fields=['rating', 'manufacturer', 'id', 'descriptionVector Vector'],
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                               "rating > 3 and "
                                                                               "manufacturer like '%%C%% and "
                                                                               'id like "%%58%%',
                                                                               f'APPROX_L2_DIST '
                                                                               f'descriptionVector vectorValue')))

        # Single vector + multiple scalar field + Partitioned
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'multiScalarOneVector_2',
                            index_fields=['rating', 'manufacturer', 'descriptionVector Vector'],
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                               "rating BETWEEN 1 and 3 AND"
                                                                               "category = BMW ",
                                                                               f'APPROX_L2_DIST '
                                                                               f'colorRGBVector vectorValue'),
                            partition_by_fields=['meta().id']
                            ))

        # Single vector + multiple scalar field with Include clause
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'multiScalarOneVector_1',
                            index_fields=['rating ', 'id', 'manufacturer', 'descriptionVector Vector'],
                            missing_indexes=True, missing_field_desc=True,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                               "rating > 3 OR "
                                                                               "manufacturer != Audi ",
                                                                               f'APPROX_L2_DIST '
                                                                               f'descriptionVector vectorValue')))
        # Partial Indexes
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'multiScalarOneVector_1',
                            index_fields=['rating ', 'description', 'descriptionVector Vector'],
                            index_where_clause='rating > 3',
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                               "rating = 4 and "
                                                                               'description like "%%Mercedes%%"',
                                                                               'APPROX_L2_DIST '
                                                                               f'descriptionVector vectorValue')))
        return definitions_list

    def generate_car_vector_loader_index_definition(self, index_name_prefix=None, similarity="L2", train_list=None,
                                                    scan_nprobes=1, skip_primary=False, array_indexes=False,
                                                    limit=10, quantization_algo_color_vector=None,
                                                    quantization_algo_description_vector=None,
                                                    is_base64=False, xattr_indexes=False, description_dimension=384):

        definitions_list = []
        color_vec_1 = [82.5, 106.700005, 20.9]  # camouflage green
        color_vec_2 = [265.1, 13.200001, 75.9]  # Pinkish red
        color_vecfield = "colorRGBVector"
        desc_vecfield = "descriptionVector"
        if xattr_indexes:
            color_vecfield = "meta().xattrs.colorVector"
            desc_vecfield = "meta().xattrs.descVector"

        desc_1 = "A convertible car with shades of green color made in 1990"
        desc_2 = "A red color car with 4 or 5 star safety rating"

        desc_vec1 = list(self.encoder.encode(desc_1))
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_color_vec_1 = f"ANN_DISTANCE({color_vecfield}, {color_vec_1}, '{similarity}', {scan_nprobes})"
        scan_color_vec_2 = f"ANN_DISTANCE({color_vecfield}, {color_vec_2}, '{similarity}', {scan_nprobes})"
        scan_desc_vec_1 = f"ANN_DISTANCE({desc_vecfield}, {desc_vec1}, '{similarity}', {scan_nprobes})"
        scan_desc_vec_2 = f"ANN_DISTANCE({desc_vecfield}, {desc_vec2}, '{similarity}', {scan_nprobes})"

        if is_base64:
            scan_color_vec_1 = (f"ANN_DISTANCE(DECODE_VECTOR({color_vecfield}, false), {color_vec_1},"
                                f" '{similarity}', {scan_nprobes})")
            scan_color_vec_2 = (f"ANN_DISTANCE(DECODE_VECTOR({color_vecfield}, false), {color_vec_2},"
                                f" '{similarity}', {scan_nprobes})")
            scan_desc_vec_1 = (f"ANN_DISTANCE(DECODE_VECTOR({desc_vecfield}, false), {desc_vec1},"
                               f" '{similarity}', {scan_nprobes})")
            scan_desc_vec_2 = (f"ANN_DISTANCE(DECODE_VECTOR({desc_vecfield},false), {desc_vec2},"
                               f" '{similarity}', {scan_nprobes})")

        if not index_name_prefix:
            index_name_prefix = "docloader" + str(uuid.uuid4()).replace("-", "")

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_scalar_{"".join(random.choices(string.ascii_uppercase + string.digits, k=5))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[], limit=limit,
                                query_template=RANGE_SCAN_TEMPLATE.format("DISTINCT color", 'colorHex like "#8f%%"'),
                                is_primary=True))
        # Single vector field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'colorRGBVector', index_fields=[f'{color_vecfield} VECTOR'],
                            dimension=3, description=f"IVF,{quantization_algo_color_vector}", similarity=similarity,
                            scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"{color_vecfield}, {scan_color_vec_1}",
                                                                              scan_color_vec_1)))

        # Single vector + single scalar field + partitioned on vector
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'oneScalarLeadingOneVector',
                            index_fields=['fuel', f'{desc_vecfield} VECTOR'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"fuel, {desc_vecfield},"
                                                                               f" {scan_desc_vec_1}",
                                                                               'fuel = "LPG" ',
                                                                               scan_desc_vec_1
                                                                               ),
                            partition_by_fields=['meta().id']))

        # Single vector (leading) + single scalar field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'oneScalarOneVectorLeading',
                            index_fields=[f'{desc_vecfield} VECTOR', 'category'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"category, {desc_vecfield}",
                                                                               'category in ["Sedan", "Luxury Car"] ',
                                                                               scan_desc_vec_2)))

        # Single vector + multiple scalar field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'multiScalarOneVector_1',

                            index_fields=['year', f'{desc_vecfield} Vector', 'manufacturer', 'fuel'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"fuel, year,"
                                                                               f" {desc_vecfield}, {scan_desc_vec_2}",
                                                                               "year between 1950 and 1990 and "
                                                                               "manufacturer in ['Chevrolet', 'Tesla',"
                                                                               " 'ford', 'Audi'] and (fuel = 'Gasoline'"
                                                                               " OR fuel= 'Electric') ",
                                                                               scan_desc_vec_2)))

        # Single vector + multiple scalar field + Partitioned
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'multiScalarOneVector_2',
                            index_fields=['rating', f'{color_vecfield} Vector', 'category'],
                            dimension=3, description=f"IVF,{quantization_algo_color_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"color, {color_vecfield}",
                                                                               "rating = 2 and "
                                                                               "category in ['Convertible', "
                                                                               "'Luxury Car', 'Supercar']",
                                                                               scan_color_vec_2),
                            partition_by_fields=['meta().id']
                            ))

        # Single vector + multiple scalar field with Include clause
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'includeMissing',
                            index_fields=['colorHex ', 'year', 'fuel', f'{desc_vecfield} Vector'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes, is_base64=is_base64,
                            train_list=train_list, limit=limit, missing_indexes=True, missing_field_desc=True,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"fuel, year, {desc_vecfield}",
                                                                               "year > 1980 OR "
                                                                               "fuel = 'Diesel' ",
                                                                               scan_desc_vec_2)))
        # Partial Indexes
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'PartialIndex',
                            index_fields=['rating ', 'color', f'{color_vecfield} Vector'],
                            index_where_clause='rating > 3',
                            dimension=3, description=f"IVF,{quantization_algo_color_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"description, {color_vecfield}",
                                                                               "rating = 4 and "
                                                                               'color like "%%blue%%" ',
                                                                               scan_color_vec_1)))

        if array_indexes:
            # Single vector + array scalar field 1
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'arrayScalarOneVector',
                                index_fields=['model',
                                              'ALL ARRAY cv.`smart features` FOR cv in evaluation.`smart features` END, ',
                                              f'{color_vecfield} Vector'],
                                dimension=384, description="IVF,PQ8x8", similarity=similarity,
                                scan_nprobes=scan_nprobes, train_list=train_list, limit=limit, is_base64=is_base64,
                                query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                                   "model = Atlas and "
                                                                                   'ANY cv in evaluation SATISFIES cv.`smart features` like "%%lights%%" ',
                                                                                   f'APPROX_L2_DIST {color_vecfield} vectorValue')))

            # Single vector + array scalar field 2
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'flatenScalarOneVector',
                                index_fields=['rating', 'ALL ARRAY  cv.variant FOR cv in evaluation END ',
                                              f'{desc_vecfield} Vector'],
                                dimension=384, description="IVF,PQ8x8", similarity=similarity,
                                scan_nprobes=scan_nprobes, train_list=train_list, limit=limit, is_base64=is_base64,
                                query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                                   "rating BETWEEN 1 and 3 AND"
                                                                                   "ANY cv in evaluation SATISFIES cv.variant = Prestige ",
                                                                                   f'APPROX_L2_DIST {desc_vecfield} vectorValue')))

            # Single vector + flatten array scalar field
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'arrayScalarOneVector',
                                index_fields=['year',
                                              'ALL ARRAY FLATTEN_KEYS(cv.comfort DESC, cv.performance) FOR cv in evaluation END ',
                                              f'{desc_vecfield} Vector'],
                                dimension=384, description="IVF,PQ8x8", similarity=similarity,
                                scan_nprobes=scan_nprobes, train_list=train_list, limit=limit, is_base64=is_base64,
                                query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                                   "year BETWEEN 1970 and 1990 AND"
                                                                                   "ANY cv in evaluation SATISFIES cv.comfort = 'Panoramic sunroof' OR "
                                                                                   "cv.performance = 'Hybrid and electric powertrains' ",
                                                                                   f'APPROX_L2_DIST {desc_vecfield} vectorValue')))

            # array vector +  multi scalar field
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'arrayScalarOneVector',
                                index_fields=['fuel',
                                              'ALL ARRAY cv.featureVectors VECTOR FOR cv in evaluation END ',
                                              'year'],
                                dimension=384, description="IVF,PQ8x8", similarity=similarity,
                                scan_nprobes=scan_nprobes, train_list=train_list, limit=limit, is_base64=is_base64,
                                query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                                   "fuel = Ethanol"
                                                                                   "AND year BETWEEN 1990 and 2000 ",
                                                                                   f'APPROX_L2_DIST {desc_vecfield} vectorValue')))

            # flatten array vector +  multi scalar field
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'arrayScalarOneVector',
                                index_fields=['fuel',
                                              'ALL ARRAY FLATTEN_KEYS(cv.featureVectors) VECTOR FOR cv in evaluation END ',
                                              'year'],
                                dimension=384, description="IVF,PQ8x8", similarity=similarity,
                                scan_nprobes=scan_nprobes, train_list=train_list, limit=limit, is_base64=is_base64,
                                query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format("*",
                                                                                   "fuel = Ethanol"
                                                                                   "AND year BETWEEN 1990 and 2000 ",
                                                                                   f'APPROX_L2_DIST {desc_vecfield} vectorValue')))
        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_shoe_vector_index_defintion_bhive(self, index_name_prefix=None, similarity="L2",
                                                   train_list=None, scan_nprobes=1, skip_primary=False,
                                                   limit=10, quantization_algo_description_vector=None,
                                                   persist_full_vector=True):
        definitions_list = []

        query_vec = f"ANN_DISTANCE(embedding, embVector,  '{similarity}', {scan_nprobes})"
        if not index_name_prefix:
            index_name_prefix = "shoe_idx_" + str(uuid.uuid4()).replace("-", "")[5]
        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=5))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[], limit=limit,
                                query_template=RANGE_SCAN_TEMPLATE.format("DISTINCT color",
                                                                          "type = 'Shoes'"),
                                is_primary=True))
        # Single vector field - embedding
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'vector_only_bhive',
                            index_fields=['embedding VECTOR'],
                            dimension=128, description=f"IVF,{quantization_algo_description_vector}", similarity=similarity,
                            scan_nprobes=scan_nprobes, persist_full_vector=persist_full_vector,
                            train_list=train_list, limit=limit,
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"embedding, {query_vec}", query_vec)))

        # Single vector field single scalar (leading)
        # definitions_list.append(
        #     QueryDefinition(index_name=index_name_prefix + 'leading_scalar',
        #                     index_fields=['embedding VECTOR'],
        #                     dimension=128, description=f"IVF,{quantization_algo_description_vector}",
        #                     similarity=similarity, persist_full_vector=persist_full_vector,
        #                     scan_nprobes=scan_nprobes, train_list=train_list, limit=limit,
        #                     query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"meta().id, {query_vec}", f"size = 5",
        #                                                                        query_vec),
        #                     include_fields=['size']))

        # # Single vector field (middle) multiple scalar (leading)
        # definitions_list.append(
        #     QueryDefinition(index_name=index_name_prefix + 'multi_scalar_middle_vec',
        #                     index_fields=['embedding VECTOR'],
        #                     dimension=128, description=f"IVF,{quantization_algo_description_vector}",
        #                     similarity=similarity, persist_full_vector=persist_full_vector,
        #                     scan_nprobes=scan_nprobes, train_list=train_list, limit=limit,
        #                     query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"meta().id, {query_vec}",
        #                                                                        "size = 5 AND color = 'Green'",
        #                                                                        query_vec),
        #                     include_fields=['size, color']))

        # # multiple scalar (leading) partitioned
        # definitions_list.append(
        #     QueryDefinition(index_name=index_name_prefix + 'multi_scalar_partitioned',
        #                     index_fields=['embedding VECTOR'],
        #                     dimension=128, description=f"IVF,{quantization_algo_description_vector}",
        #                     similarity=similarity, partition_by_fields=['meta().id'], scan_nprobes=scan_nprobes,
        #                     train_list=train_list, limit=limit, persist_full_vector=persist_full_vector,
        #                     query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"meta().id, {query_vec}, color",
        #                                                                        "size > 4 AND size < 6 AND brand = 'Nike'",
        #                                                                        query_vec),
        #                     include_fields=['size, color, brand']))

        # # vector leading multiple scalar (leading) partitioned by vector
        # definitions_list.append(
        #     QueryDefinition(index_name=index_name_prefix + 'leading_vec_partitioned',
        #                     index_fields=['embedding VECTOR'],
        #                     dimension=128, description=f"IVF,{quantization_algo_description_vector}",
        #                     similarity=similarity, partition_by_fields=['embedding'], scan_nprobes=scan_nprobes,
        #                     train_list=train_list, limit=limit, persist_full_vector=persist_full_vector,
        #                     query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"meta().id, {query_vec}, type",
        #                                                                        "size < 6 AND country = 'USA'",
        #                                                                        query_vec),
        #                     include_fields=['size, color, brand, country, type']))

        return definitions_list

    def generate_car_vector_loader_index_definition_bhive(self, index_name_prefix=None, similarity="L2",
                                                          train_list=None, xattr_indexes=False,
                                                          scan_nprobes=1, skip_primary=False, array_indexes=False,
                                                          limit=10, quantization_algo_color_vector=None,
                                                          quantization_algo_description_vector=None):

        definitions_list = []
        color_vec_1 = [82.5, 106.700005, 20.9]  # camouflage green
        color_vec_2 = [265.1, 13.200001, 75.9]  # Pinkish red
        color_vecfield = "colorRGBVector"
        desc_vecfield = "descriptionVector"
        if xattr_indexes:
            color_vecfield = "meta().xattrs.colorVector"
            desc_vecfield = "meta().xattrs.descVector"

        desc_1 = "A convertible car with red color made in 1990"
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        descVec1 = list(self.encoder.encode(desc_1))
        descVec2 = list(self.encoder.encode(desc_2))

        scan_color_vec_1 = f"ANN_DISTANCE({color_vecfield}, {color_vec_1}, '{similarity}', {scan_nprobes}, false)"
        scan_color_vec_2 = f"ANN_DISTANCE({color_vecfield}, {color_vec_2}, '{similarity}', {scan_nprobes}, true)"
        scan_desc_vec_1 = f"ANN_DISTANCE({desc_vecfield}, {descVec1}, '{similarity}', {scan_nprobes}, false)"
        scan_desc_vec_2 = f"ANN_DISTANCE({desc_vecfield}, {descVec2}, '{similarity}', {scan_nprobes}, true)"

        if not index_name_prefix:
            index_name_prefix = "docloader" + str(uuid.uuid4()).replace("-", "")

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_scalar_{"".join(random.choices(string.ascii_uppercase + string.digits, k=5))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[], limit=limit,
                                query_template=RANGE_SCAN_TEMPLATE.format("DISTINCT color", 'colorHex like "#8f%%"'),
                                is_primary=True))

        # Single vector field - colorRGBVector
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'colorRGBVectorBhive',
                            index_fields=[f'{color_vecfield} VECTOR'],
                            dimension=3, description=f"IVF,{quantization_algo_color_vector}", similarity=similarity,
                            scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit,
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"{color_vecfield},"
                                                                              f" {scan_color_vec_1}",
                                                                              scan_color_vec_1)))

        # Single vector field - descriptionVector
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'descriptionVectorBhive',
                            index_fields=[f'{desc_vecfield} VECTOR'],
                            dimension=384, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, persist_full_vector=False,
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"{desc_vecfield},"
                                                                              f" {scan_desc_vec_1}",
                                                                              scan_desc_vec_2)))

        # Single vector field + multiple scalar fields + partitioned on scalar field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitionedVectorBhive',
                            index_fields=[f'{desc_vecfield} VECTOR'],
                            dimension=384, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"{desc_vecfield}",
                                                                               "rating = 2 and "
                                                                               "category in ['Convertible', "
                                                                               "'Luxury Car', 'Supercar']",
                                                                               scan_desc_vec_1),
                            partition_by_fields=['meta().id'], include_fields=['rating', 'category']
                            ))

        # Single vector + multiple scalar fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'includeBhive',
                            index_fields=[f'{color_vecfield} VECTOR'],
                            dimension=3, description=f"IVF,{quantization_algo_color_vector}", similarity=similarity,
                            scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, persist_full_vector=False,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"{color_vecfield}",
                                                                               "year > 1980 OR "
                                                                               "fuel = 'Diesel' ",
                                                                               scan_color_vec_2),
                            include_fields=['fuel', 'year']))
        # Partial Indexes
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partialIndexBhive',
                            index_fields=[f'{desc_vecfield} VECTOR'],
                            index_where_clause='rating > 3',
                            dimension=384, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"description, {desc_vecfield}",
                                                                               "rating = 4 and "
                                                                               'description like "%%Convertible%%"',
                                                                               scan_desc_vec_2),
                            include_fields=['description', 'rating']))
        return definitions_list

    def generate_magma_doc_loader_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "docloader" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'name', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'name is not null')))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "attributes.dimensions.height > 40"),
                                is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'age_gender_name',
                            index_fields=['age', 'gender', 'name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'age > 40 AND '
                                                                      'gender = "M"')))

        # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys', index_fields=['name', 'age',
                                                                                         'marital'],
                            missing_indexes=True, missing_field_desc=False,
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'age > 30 AND '
                                                                      'marital = "S"')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['name', 'body'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'body like "%%E%%"'),
                            partition_by_fields=['body'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index',
                            index_fields=['mutated', 'name', 'body',
                                          'ALL ARRAY h.name FOR h IN attributes.hobbies END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'mutated >= 0 and '
                                                                      'ANY h IN attributes.hobbies SATISFIES'
                                                                      ' h.name = "Books" END')))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_2',
                            index_fields=['age', 'name', 'ALL animals'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'any a in animals satisfies '
                                                                      'a = "Forest green (traditional)" end')))

        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_employee_data_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "employee" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'name', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'name = "employee-1" ')))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "test_rate > 1"), is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'join_day_job_title',
                            index_fields=['join_day', 'job_title'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'join_day > 15 AND '
                                                                      'job_title = "Sales"')))

        # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys', index_fields=['email', 'join_yr',
                                                                                         'join_mo'],
                            missing_indexes=True, missing_field_desc=False,
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'join_yr > 2010 AND '
                                                                      'join_mo > 6')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['job_title'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'job_title = "Support"'),
                            partition_by_fields=['job_title'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index',
                            index_fields=['join_mo, All ARRAY vm.os FOR vm IN VMs END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'ANY vm IN VMs SATISFIES vm.os = "ubuntu" '
                                                                      'END')))
        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_car_data_index_definition_scalar(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "car" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query - price
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'price', index_fields=['price'],
                            query_template=RANGE_SCAN_TEMPLATE.format("price", "price > 0")))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("id", "id is not NULL"),
                                is_primary=True))

        # GSI index on multiple fields - basic car details
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'manufacturer_model_year',
                            index_fields=['manufacturer', 'model', 'year'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'year > 1900 AND '
                                                                      'manufacturer = "Hyundai"')))

        # GSI index on rating and category
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'rating_category',
                            index_fields=['rating', 'category'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'rating > 2 AND '
                                                                      'category = "Subcompact"')))

        # GSI index with array fields - evaluation features
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'smart_features',
                            index_fields=['DISTINCT ARRAY sf FOR sf IN evaluation[0].`smart features` END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'ANY sf IN evaluation[0].`smart features` '
                                                                      'SATISFIES sf LIKE "%%display%%" END')))
        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_hotel_data_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "hotel" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'price', index_fields=['price'],
                            query_template=RANGE_SCAN_TEMPLATE.format("price", "price > 0")))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("suffix", "suffix is not NULL order by suffix"),
                                is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'free_breakfast_avg_rating',
                            index_fields=['free_breakfast', 'avg_rating'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'avg_rating > 3 AND '
                                                                      'free_breakfast = true '
                                                                      'order by name')))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'free_breakfast_array_count',
                            index_fields=['free_breakfast', 'type', 'free_parking', 'array_count(public_likes)',
                                          'price', 'country'],
                            query_template=RANGE_SCAN_TEMPLATE.format(
                                "country, avg(price) as AvgPrice, min(price) as MinPrice,"
                                " max(price) as MaxPrice",
                                "free_breakfast=True and free_parking=True and "
                                "price is not null and "
                                "array_count(public_likes)>5 and "
                                "`type`='Hotel' group by country order by country")))

        # # GSI index with Flatten Keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'flatten_keys',
                            index_fields=['DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness)'
                                          ' FOR r IN reviews when r.ratings.Cleanliness < 4 END',
                                          'country', 'email', 'free_parking'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      "ANY r IN reviews SATISFIES r.author LIKE 'M%%' "
                                                                      "AND r.ratings.Cleanliness = 3 END AND "
                                                                      "free_parking = TRUE AND country IS NOT NULL order by name")))

        # # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys',
                            index_fields=['city', 'avg_rating', 'country'],
                            missing_indexes=True, missing_field_desc=True,
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'avg_rating > 3 AND '
                                                                      'country like "%%F%%" order by country')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name", 'name like "%%Dil%%" order by name'),
                            partition_by_fields=['name'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_overall',
                            index_fields=['price, All ARRAY v.ratings.Overall FOR v IN reviews END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("address", 'ANY v IN reviews SATISFIES v.ratings.'
                                                                                 '`Overall` > 3  END and price < 1000 order by address')))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_rooms',
                            index_fields=['price, All ARRAY v.ratings.Rooms FOR v IN reviews END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'ANY v IN reviews SATISFIES v.ratings.'
                                                                      '`Rooms` > 3  END and price > 1000 order by name')))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_checkin',
                            index_fields=['country', 'DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` '
                                                     'FOR r in `reviews` END', 'array_count(`public_likes`)',
                                          'array_count(`reviews`) DESC', '`type`', 'phone', 'price', 'email',
                                          'address', 'name', 'url'],
                            query_template=RANGE_SCAN_TEMPLATE.format("address",
                                                                      'country is not null and `type` is not null '
                                                                      'and (any r in reviews satisfies '
                                                                      'r.ratings.`Check in / front desk` '
                                                                      'is not null end) order by country')))

        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_exhaustive_hotel_data_index_definition(self, index_name_prefix=None):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "hotel" + str(uuid.uuid4()).replace("-", "")

        # # Default primary Index + num_replica
        # definitions_list.append(
        #     QueryDefinition(index_name=None, index_fields=[],
        #                     query_template=RANGE_SCAN_TEMPLATE.format("suffix", "suffix is not NULL"),
        #                     is_primary=True, num_replica=1))# Default primary Index + num_replica
        # definitions_list.append(
        #     QueryDefinition(index_name=None, index_fields=[],
        #                     query_template=RANGE_SCAN_TEMPLATE.format("suffix", "suffix is not NULL"),
        #                     is_primary=True, num_replica=1))
        # Primary Index
        prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
        definitions_list.append(
            QueryDefinition(index_name=prim_index_name, index_fields=[],
                            query_template=RANGE_SCAN_TEMPLATE.format("suffix", "suffix is not NULL"),
                            is_primary=True, defer_build=False))

        # Primary index + deferred + partitioned
        prim_index_defer = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
        definitions_list.append(
            QueryDefinition(index_name=prim_index_defer, index_fields=[],
                            query_template=RANGE_SCAN_TEMPLATE.format("suffix", "suffix is not NULL"),
                            is_primary=True, defer_build=True, partition_by_fields=['meta().id']))

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'price', index_fields=['price'],
                            query_template=RANGE_SCAN_TEMPLATE.format("price", "price > 0")))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'free_breakfast_avg_rating',
                            index_fields=['free_breakfast', 'avg_rating'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'avg_rating > 3 AND '
                                                                      'free_breakfast = true')))

        # GSI index on multiple fields with aggregate functions
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'free_breakfast_array_count',
                            index_fields=['free_breakfast', 'type', 'free_parking', 'array_count(public_likes)',
                                          'price', 'country'],
                            query_template=RANGE_SCAN_TEMPLATE.format(
                                "country, avg(price) as AvgPrice, min(price) as MinPrice,"
                                " max(price) as MaxPrice",
                                "free_breakfast=True and free_parking=True and "
                                "price is not null and "
                                "array_count(public_likes)>5 and "
                                "`type`='Hotel' group by country order by country")))

        # GSI index with include missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys',
                            index_fields=['city', 'avg_rating', 'country'],
                            missing_indexes=True, missing_field_desc=True,
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'avg_rating > 3 AND '
                                                                      'country like "%%F%%"')))

        # GSI Index with partial indexes
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partialIndex',
                            index_fields=['rating ', 'color'],
                            index_where_clause='rating > 3',
                            query_template=RANGE_SCAN_TEMPLATE.format("rating, color",
                                                                      "rating = 4 and "
                                                                      'color like "%%blue%%" ')))

        # GSI array index with ALL
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_overall',
                            index_fields=['price, All ARRAY v.ratings.Overall FOR v IN reviews END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("address", 'ANY v IN reviews SATISFIES v.ratings.'
                                                                                 '`Overall` > 3  END and price < 1000 ')))

        # GSI array index with DISTINCT
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_Distinct',
                            index_fields=['avg_rating, DISTINCT ARRAY v.ratings.Rooms FOR v IN reviews END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'ANY v IN reviews SATISFIES v.ratings.'
                                                                      '`Rooms` > 3  END and avg_rating >= 3 ')))

        # GSI partial array index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_partial',
                            index_fields=['price,'
                                          ' All ARRAY v.ratings.Cleanliness FOR v IN reviews when v.ratings.overall > 2 END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("address", 'ANY v IN reviews SATISFIES v.ratings.'
                                                                                 '`Cleanliness` > 3  END and price < 1000 ')))

        # GSI index with Flatten Keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'flatten_keys',
                            index_fields=['DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness)'
                                          ' FOR r IN reviews when r.ratings.Cleanliness < 4 END',
                                          'country', 'email', 'free_parking'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      "ANY r IN reviews SATISFIES r.author LIKE 'M%%' "
                                                                      "AND r.ratings.Cleanliness = 3 END AND "
                                                                      "free_parking = TRUE AND country IS NOT NULL ")))

        # GSI Array Index with include missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_missing_keys',
                            index_fields=['city', 'avg_rating', 'country',
                                          'ALL ARRAY r.ratings.Cleanliness FOR r IN reviews END'],
                            missing_indexes=True, missing_field_desc=True,
                            query_template=RANGE_SCAN_TEMPLATE.format("city, avg_rating, country",
                                                                      'avg_rating > 3 AND '
                                                                      'ANY v IN reviews SATISFIES v.ratings.Cleanliness > 3 AND '
                                                                      'country like "%%F%%"')))

        # GSI Array Index on compound object
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_on_compound_object',
                            index_fields=['price, All ARRAY [v.ratings.Overall, v.ratings.`Value`] FOR v IN reviews END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("address", 'ANY v IN reviews SATISFIES [v.ratings.'
                                                                      '`Overall`, v.ratings.`Value`] = [2, 4] END and price < 1000 ')))


        # GSI Partial Index on metadata
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'metadata_index',
                            index_fields=['meta().id', 'meta().expiration'],
                            index_where_clause='meta().expiration > 0',
                            query_template=RANGE_SCAN_TEMPLATE.format("meta().id", 'meta().expiration > 0')))

        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_hotel_data_index_definition_vectors(self, index_name_prefix=None, similarity="L2",
                                                     train_list=None,
                                                     scan_nprobes=1,
                                                     limit=10, quantization_algo_color_vector=None,
                                                     quantization_algo_description_vector=None, is_base64=False,
                                                     description_dimension=384):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "hotel" + str(uuid.uuid4()).replace("-", "")

        vector = [2.5273438, -0.34814453, 1.1171875, 1.0703125, 0.7084961, -0.15429688, -1.2890625, 0.55078125,
                  -2.3359375, -0.70703125, 0.068603516, 1.7333984, -0.11010742, -1.1875, 1.2519531, 0.99658203,
                  -0.94970703, -0.9819336, 0.14758301, -1.1748047, -1.1767578, 3.84375, -0.64208984, 0.32714844,
                  0.55029297, -0.6152344, -0.7636719, -0.2980957, -0.032440186, 1.4824219, -1.2451172, -1.2314453,
                  -1.3095703, -0.92626953, -1.8994141, -0.2265625, -0.04724121, -0.49780273, -1.5761719, -0.32348633,
                  1.2802734, 1.4550781, -0.015701294, -0.018554688, -1.8212891, 0.7373047, 0.30737305, 0.5415039,
                  -2.1230469, 1.4648438, 1.3964844, -1.5273438, 0.09710693, 0.67871094, -1.75, 0.87890625, 0.99902344,
                  2.1464844, -0.52246094, 0.048675537, -0.7939453, 0.8881836, -0.40625, 0.1821289, -1.1875, 0.21960449,
                  -2.5488281, -3.2578125, 2.7832031, 1.421875, 4.0390625, -0.08129883, -0.51660156, 0.16638184,
                  1.5048828, 1.7080078, 0.6743164, -1.0185547, -0.044830322, 0.14685059, -2.6191406, 0.39233398,
                  -1.8193359, 0.9038086, -0.35742188, 0.77197266, -0.39526367, 1.1386719, 0.15283203, 3.3847656,
                  -0.8378906, -0.9824219, -0.82470703, -0.27612305, -0.9140625, 1.5039062, 1.0253906, 2.5234375,
                  -2.0605469, -0.5385742, 0.94873047, 0.9765625, 1.6318359, 1.3544922, -0.9790039, 0.7885742,
                  0.45361328, 2.09375, -0.017227173, -1.7714844, 1.5732422, 0.94091797, -2.2636719, -0.86376953,
                  0.5151367, -1.2861328, 2.3144531, -4.015625, 1.1074219, 0.14733887, -1.4511719, 0.47827148,
                  0.10614014, -0.21032715, 0.030731201, 0.15356445, 0.7553711, 0.5097656, 1.0253906, 0.51464844,
                  0.56640625, -1.6044922, -1.1074219, 0.5644531, 0.10852051, 1.9179688, 0.49267578, -0.35888672,
                  -1.8085938, 1.6142578, 2.6660156, 0.5839844, 0.5625, 1.4902344, -3.4296875, -1.9726562, -1.4091797,
                  0.5703125, 0.92089844, -1.9804688, -0.81689453, -0.98095703, -0.34814453, 1.1992188, 3.7773438,
                  -0.83984375, 1.3085938, 0.47045898, -0.6748047, -0.265625, -1.8994141, 2.7792969, -0.3942871,
                  1.5166016, 0.40063477, -0.24743652, -0.20715332, 2.9082031, -1.6708984, 1.1884766, 0.16516113,
                  0.29614258, -1.1054688, -0.21276855, -0.31225586, 1.1318359, 1.6621094, -0.9042969, -2.3300781,
                  -1.5673828, -0.4675293, 0.080444336, 2.0625, 1.0224609, -0.85058594, 0.78515625, 0.16711426,
                  0.40576172, -0.4033203, -0.13867188, 1.0703125, 0.15478516, 0.5800781, 0.08605957, 0.42016602,
                  0.122802734, 1.6347656, -0.90966797, 0.44873047, -0.7114258, -1.4179688, -0.91845703, -0.5957031,
                  1.0732422, 2.6113281, -0.66308594, 2.5507812, -0.81347656, 1.5371094, 0.16345215, -1.4101562,
                  0.99902344, -0.28881836, -0.029754639, -1.2275391, 1.4863281, -1.2197266, 0.8886719, -0.3798828,
                  -1.4960938, 0.234375, -0.71435547, -1.0888672, 0.2705078, -1.578125, 0.024307251, 1.8974609,
                  -2.7226562, 0.4345703, 2.2890625, 0.8198242, -1.1699219, -2.40625, -0.4934082, -0.16772461,
                  -1.2373047, 0.16357422, -1.5673828, -1.9550781, 0.03955078, -0.09777832, 1.6269531, -0.03753662,
                  -0.15698242, 0.7597656, 1.1689453, -1.5136719, 0.30029297, 0.9663086, -2.2890625, -2.8886719,
                  -0.06958008, 3.1796875, 0.55029297, -0.44799805, 0.22631836, 1.8789062, -0.82910156, -1.2089844,
                  -0.9511719, -5.7382812, -1.7861328, 0.8876953, 0.24157715, -0.7910156, -0.98339844, -0.30029297,
                  -0.7739258, 1.4365234, -3.1308594, -0.25024414, 1.9755859, 0.66259766, -0.31323242, -0.55371094,
                  0.5913086, -4.625, -0.48046875, -0.87060547, 1.2773438, 2.7890625, 0.35839844, -2.7539062, 1.2636719,
                  0.5834961, -0.6464844, 0.2763672, 0.14123535, 3.7636719, 0.24475098, -2.1171875, -0.48706055,
                  1.4980469, 0.7963867, 0.15185547, 0.17199707, 0.043945312, 1.7792969, -0.64453125, 0.066101074,
                  0.8022461, -0.15612793, -1.5244141, -1.171875, 0.8457031, 1.2695312, -0.21240234, 1.1708984,
                  0.04220581, -0.20898438, -0.20446777, -0.14685059, -0.055725098, -1.3505859, -1.2285156, -0.31445312,
                  -1.0097656, 0.8959961, 1.6279297, 1.1123047, -3.0664062, -0.6376953, 1.6611328, 0.041809082,
                  -0.7314453, 1.4765625, -2.2421875, -0.80810547, 0.4465332, 0.15588379, 1.6005859, -1.21875,
                  -1.5224609, -0.33007812, 0.640625, -1.0234375, -2.9023438, 3.1191406, -1.8232422, 0.55029297,
                  0.6401367, 0.55126953, -1.6523438, 1.1855469, -0.8227539, -2.5664062, 0.60546875, -1.0595703,
                  0.13085938, 0.12176514, -2.3554688, 0.08288574, -0.59375, 0.640625, -0.49316406, 0.8959961,
                  -1.0429688, -2.2421875, 0.9555664, -1.7753906, 0.19384766, 0.9628906, 0.14013672, -0.7753906,
                  3.1367188, -1.9042969, -0.46875, 1.4306641, -0.23303223, -0.38256836, 0.15893555, -0.7006836,
                  1.2861328, 0.43139648, 0.07763672, -1.0976562, -1.5166016, -2.53125, -1.1835938, -2.1289062,
                  0.6616211, 1.3945312, 0.24072266, -0.6689453, 1.2675781, 0.8544922, -0.78759766, -2.2890625,
                  -1.5595703, 2.4765625, 0.12487793, 1.6748047, -0.39941406, -1.6845703, -2.9257812, 1.3769531,
                  0.19763184, -2.1738281, -0.047088623, -0.5058594, -0.29296875, -0.3942871, 0.8051758, -0.29760742,
                  0.7138672, -0.46362305, 0.45703125, 1.2197266, -1.6660156, -0.17907715, -0.30493164, -1.1591797,
                  -0.58935547, -1.3261719, 0.5854492, 3.2734375, -0.7373047, 2.921875, 0.6972656, -0.07305908,
                  -0.7661133, -0.53027344, 0.3334961, 0.4970703, -1.3359375, -1.1933594, -0.98779297, -0.61328125,
                  1.3476562, 0.4260254, 2.1699219, -0.8041992, -0.671875, -0.041137695, 0.22607422, 1.5751953,
                  0.18798828, -2.3808594, -1.0449219, 1.3310547, -0.04309082, 0.7988281, -0.5073242, 0.40673828,
                  1.0976562, -2.3945312, 0.27441406, -1.3544922, 0.43164062, -1.1845703, 0.78564453, -0.36572266,
                  1.1162109, -1.6835938, -0.053649902, -0.921875, -2.5019531, -2.6523438, 0.46484375, -0.65771484,
                  0.54785156, -1.0917969, 0.38354492, 1.2675781, -0.3786621, 0.4873047, -0.031951904, -1.7333984,
                  -0.7167969, -1.5537109, 2.5585938, -0.11413574, -0.54785156, -0.36083984, -3.5136719, -0.9042969,
                  0.20056152, -2.1503906, -0.91845703, -2.2753906, -1.4814453, -0.068725586, -2.8652344, -0.36987305,
                  0.42797852, 0.1459961, -1.7587891, 1.9101562, -0.1149292, -0.42211914, 0.74072266, -0.60498047,
                  -1.9111328, -1.9287109, -0.4008789, 0.6665039, 0.2052002, 0.024230957, 1.1035156, -0.8378906,
                  0.6977539, -0.19555664, -0.7675781, -0.8383789, 0.99316406, 0.34179688, 0.44970703, 0.101623535,
                  0.73828125, -0.6972656, 0.059509277, 0.86621094, 0.8334961, 1.5605469, -1.0429688, 0.3798828,
                  -1.1679688, 0.21728516, -2.2578125, -1.0976562, -0.09875488, 0.8964844, -0.16430664, 0.42797852,
                  -0.75, 0.13842773, -2.0742188, -1.7255859, 0.5449219, 1.2773438, 2.390625, 0.19763184, 0.6074219,
                  -0.86621094, -0.25097656, 0.8823242, 1.0488281, 1.703125, 1.0605469, -2.3125, 2.5683594, -0.14892578,
                  0.17651367, -2.6425781, -0.79345703, 0.890625, 0.17041016, -2.0449219, 0.47143555, -1.3671875,
                  0.20446777, -0.33447266, -1.0615234, -1.2851562, 2.7695312, -1.3535156, -0.116760254, 0.796875,
                  0.4038086, 0.28735352, -1.0576172, 1.0576172, -0.06542969, -0.8676758, 0.7910156, -0.5751953,
                  -1.5996094, 0.18652344, 0.26660156, -2.7285156, -0.76708984, 1.6865234, -1.5136719, -1.3261719,
                  -3.3515625, 0.6347656, -1.1855469, -0.9086914, -0.1850586, -0.17858887, 0.008712769, 0.7836914,
                  -0.4099121, -2.5996094, 3.21875, 0.5776367, -0.16894531, 1.1826172, 1.1523438, -1.3466797, -2.078125,
                  2.46875, -1.7167969, -1.0664062, 1.7431641, -0.31982422, -2.5957031, -0.012275696, 0.1730957,
                  0.36499023, 0.8066406, -1.1679688, -0.2763672, 3.4414062, -0.25830078, -0.11804199, 0.037017822,
                  0.6010742, 0.5541992, -0.7871094, 0.8696289, -0.9194336, -0.39013672, 2.1914062, -1.7617188,
                  1.5166016, -1.34375, -1.4941406, 0.5678711, 3.1015625, -0.56689453, -3.3632812, 0.5161133, -0.3491211,
                  0.5605469, 0.47143555, -2.8925781, -0.6616211, -0.62402344, 0.7885742, 0.45263672, -1.0048828,
                  3.2480469, 1.9335938, -3.1953125, 2.3359375, -3.1054688, 1.5273438, 0.72509766, -2.0625, 0.5371094,
                  0.40112305, -0.037261963, -3.5585938, -0.8027344, 0.1237793, -1.7470703, -1.1640625, 0.40844727,
                  0.23828125, -1.7460938, 0.99121094, 4.234375, -2.8242188, -0.88427734, 0.6464844, -0.7685547,
                  -1.2158203, 1.1982422, 1.3242188, 1.6083984, 0.3894043, -0.50683594, -0.53271484, -0.055603027,
                  0.85498047, -0.4267578, 1.4521484, 0.014976501, -1.4257812, 0.07495117, 0.3779297, 1.1230469,
                  -1.3056641, 2.90625, -0.11468506, -0.085632324, -0.46313477, 0.3876953, -0.63378906, 3.1679688,
                  1.3066406, 0.32373047, 1.8232422, -0.9589844, -0.046783447, -0.60595703, -2.8457031, 0.09082031,
                  1.9980469, 0.32177734, 1.8105469, -1.2675781, 0.7216797, -0.59375, 0.049438477, -3.1347656, 3.40625,
                  -1.0498047, -1.1591797, 0.43725586, 0.53515625, -1.0048828, 1.7919922, 0.26098633, 0.77978516,
                  1.2333984, 0.2019043, -4.25, -1.1708984, -0.13842773, 0.81591797, -1.2158203, -0.6220703, -0.54296875,
                  1.7353516, 1.0625, 0.1451416, -0.051086426, 0.48876953, 1.703125, -1.7949219, -1.1367188, -1.0830078,
                  -0.53271484, 1.9130859, -2.1347656, -0.5234375, -1.0009766, 0.2578125, 1.0117188, -0.72998047,
                  -2.5078125, 2.3066406, -4.40625, -0.6425781, -1.84375, -0.9765625, -2.9628906, -0.36621094, 1.7949219,
                  1.0703125, 0.48950195, -1.7119141, -0.34472656, 1.2988281, -2.3691406, 0.18273926, -0.4345703,
                  -0.088134766, 0.6503906, 1.515625, -1.0830078, -0.4465332, -1.3945312, 2.6171875, 0.49658203,
                  -1.9619141, 0.6621094, -0.04800415, 0.107177734, 0.13256836, -1.0507812, -0.42407227, 1.5527344,
                  0.43164062, -0.44628906, -1.4765625, 1.1074219, -1.1328125, -0.6455078, -1.7421875, 1.2382812,
                  -0.9082031, 0.48535156, -1.0859375, 1.3886719, 1.3398438, -0.10211182, 0.28833008, -1.3486328,
                  -1.3203125, 0.5991211, -2.2363281, 1.6357422, -0.7944336, 1.1640625, -1.1533203, -1.1621094,
                  0.46191406, -0.68310547, 0.049346924, 1.9335938, 1.4482422, -1.5859375, -0.7163086, -2.2265625,
                  -0.8828125, -0.28735352, -0.6430664, 0.5415039, -0.11999512, 2.7226562, 0.87158203, -1.9130859,
                  -1.2626953, -1.0332031, -0.0034446716, 0.6928711, -0.68066406, -1.5263672, -0.9785156, -1.9912109,
                  0.82177734, -1.2871094, -0.08081055, 0.7006836, 1.328125, 0.9399414, -1.2568359, 0.33618164,
                  0.9433594, -0.97558594, 1.2382812, -0.73095703, -1.90625, -1.1611328, -1.625, 0.91503906,
                  -0.0030956268, 1.1035156, -0.63720703, 2.0390625, -0.16540527, -0.26464844, 0.06878662, 1.6904297,
                  3.1425781, -0.43847656, -0.16064453, 0.25756836, -1.6826172, -0.14208984, 0.8051758, 0.07940674,
                  -4.640625, -0.10369873, 1.3427734, 0.12573242, 0.88427734, -2.0820312, 1.5048828, 0.040374756,
                  -1.5693359, -0.48046875, -1.4570312, 1.5058594, 1.1523438, -0.30444336, 0.52490234, 3.8574219,
                  -0.7910156, 0.47705078, -0.035247803, -1.3603516, 0.07525635, -0.8774414, -0.02407837, -1.0361328,
                  -2.7734375, -0.49047852, 0.54833984, -0.4765625, 1.5351562, -1.7773438, 2.7441406, 1.6943359,
                  -1.5380859, 1.0498047, 3.4726562, 1.2041016, -0.19665527, -1.0566406, -1.2294922, 1.6582031,
                  -1.0703125, -0.6616211, -1.8193359, 1.8261719, -2.4570312, -1.1904297, 2.7734375, 1.0048828,
                  -1.3613281, -0.5756836, 1.8544922, 0.92089844, -0.82470703, 1.1591797, -0.0067825317, 0.2529297,
                  -2.1445312, -3.1777344, -1.3818359, -0.33032227, -0.39501953, -0.5722656, -1.7695312, 3.125,
                  0.20458984, -1.0732422, 2.4296875, -1.1386719, -0.23608398, -1.0175781, -2.3984375, 0.8964844,
                  0.7529297, 1.3808594, 1.0126953, -0.35839844, 1.1904297, -2.7753906, -0.87597656, -1.5488281,
                  0.87158203, 0.7207031, -0.5917969, -1.2705078, -1.0302734, 0.8652344, -0.92626953, -1.3710938,
                  1.0878906, -1.7675781, -0.3659668, -0.40063477, -0.19091797, 0.73339844, 1.0869141, -0.2944336,
                  -0.14172363, 1.9130859, 1.2050781, 0.54052734, -1.2802734, 0.4716797, -1.8300781, 0.8051758,
                  2.1953125, -0.65234375, 0.89941406, 1.5595703, -0.115112305, -0.48388672, -0.72753906, 0.27270508,
                  -2.4824219, 0.041870117, 1.1816406, 1.9257812, 0.9086914, 0.24719238, 2.9511719, -2.9433594,
                  -0.14929199, -2.0527344, -0.28857422, -2.0078125, 1.2978516, 1.0869141, -0.42871094, 1.5058594,
                  0.8339844, -2.0039062, -0.6225586, 1.5039062, 2.015625, -0.17407227, 0.8261719, 1.2753906, 2.0839844,
                  -0.47265625, -1.3935547, -1.2021484, -0.5961914, 1.90625, 0.24523926, 0.13220215, -0.30297852,
                  -1.5898438, 0.48779297, 0.037139893, 0.050994873, -1.2197266, 0.9316406, 1.2138672, -0.24853516,
                  -0.037597656, -2.2773438, 0.40039062, 0.46972656, -0.7246094, 2.7675781, -0.9008789, 0.19348145,
                  0.34545898, -2.6054688, 1.8349609, -2.0546875, 2.0625, 1.34375, 1.5039062, -1.3886719, 0.048858643,
                  -0.9238281, 1.5585938, 1.2617188, 0.7421875, 3.5507812, -0.3491211, -1.2929688, -0.7680664,
                  -1.8662109, -0.5839844, 1.5117188, 0.17675781, 0.26049805, -2.9101562, -0.0055351257, -1.0566406,
                  1.5322266, -2.8730469, 0.46606445, 0.13134766, -3.0859375, -1.4394531, -0.03048706, -1.4580078,
                  -1.2138672, -0.59472656, 2.0527344, -0.8984375, -1.0839844, 2.4511719, 0.50341797, 2.953125,
                  -1.5302734, 0.6796875, 0.0803833, 0.056518555, 1.90625, 0.54052734, -0.18798828, 1.9335938,
                  -1.3818359, 1.1416016, 2.6972656, 1.2949219, 1.8056641, -1.7578125, -0.21557617, -1.1757812,
                  0.26611328, -0.66064453, -0.98339844, -0.25463867, 3.2773438, -0.73876953, -0.12011719, 0.10864258,
                  -0.9350586, -0.3010254, 2.4394531, 0.36987305, -0.12133789, 2.0292969, 1.3984375, -0.40698242,
                  1.5429688, -0.61816406, 1.40625, 0.14355469, -1.2939453, 0.34277344, -1.7236328, 2.3398438,
                  -0.7080078, 0.75927734, -0.6616211, 0.5488281, -0.018798828, -1.9736328, 0.03729248, 0.60791016,
                  1.3046875, 0.7089844, 1.4492188, -1.2548828, -0.94189453, -0.02999878, -0.9980469, 0.004776001,
                  1.5185547, -0.37280273, -0.71728516, 0.55908203, -1.9667969, 2.3164062, -0.828125, 2.7109375,
                  -1.7548828, 2.7460938, 0.6972656, -0.27148438, 0.6064453, -1.6132812, 0.32421875, -0.6088867,
                  -0.10217285, 1.4970703, 0.6479492, 2.1328125, 0.87158203, -2.0488281, -1.0390625, 1.2021484,
                  0.09020996, 1.1621094, 0.28027344, -0.89160156, 0.13659668, 0.7104492, 0.9946289, 0.4716797,
                  -0.8623047, 1.0341797, -0.99316406, -0.74902344, 1.6445312, 0.013702393, -0.06097412, 0.29589844,
                  1.6787109, 1.8369141, 0.15490723, 0.5283203, -0.47998047, -0.13684082, -0.40234375, -0.24108887,
                  5.5625, -3.1933594, -0.6855469, 0.17126465, -1.0673828, -1.6894531, 1.0429688, 2.4648438, -2.3535156,
                  1.1152344, 1.0654297, 1.5556641, -1.0429688, -1.7851562, -1.2919922, -1.1884766, 4.4101562, 1.9726562,
                  -1.0996094, 1.7666016, 0.6816406, 1.4570312, -0.0031204224, -0.53564453, -2.3046875, -0.7636719,
                  -0.31469727, 1.1728516, 0.40454102, -0.90185547, -0.14404297, -1.6005859, -1.8974609, -1.015625,
                  0.25683594, 0.9316406, -0.70410156, -0.93408203, -0.7519531, -0.23608398, -0.23132324, -0.22766113,
                  0.8125, 2.8613281, -1.6835938, 1.8857422, 1.9179688, -0.5527344, 0.03353882, 4.0039062, 0.69384766,
                  -0.8930664, 0.5488281, 0.00283432, 0.14575195, -0.79248047, 3.1210938, -0.45654297, 3.0175781,
                  0.28759766, 3.5214844, -0.17626953, -1.5019531, -1.0771484, -0.8491211, 1.3974609, -0.05532837,
                  -1.265625, -0.5019531, 1.1464844, -0.75146484, -0.6357422, 1.0947266, -1.9755859, -0.97753906,
                  -0.62597656, -0.68652344, 0.023620605, 0.41992188, -1.0419922, 1.0263672, -1.3056641, -1.3232422,
                  -1.2666016, -1.3271484, 0.5571289, 0.75927734, -0.8984375, 1.5966797, 0.87597656, -0.013023376,
                  -0.8745117, -1.9912109, -0.5830078, 0.58496094, -0.19360352, 0.30517578, 3.4433594, -3.4121094,
                  -0.56884766, 0.3479004, 0.6665039, -1.7451172, 2.3242188, -2.4960938, 0.9511719, -0.5620117,
                  -0.60009766, 0.9511719, 1.4960938, 3.2480469, -0.70654297, -2.7519531, -1.5390625, 0.4453125,
                  2.7558594, 1.1669922, -2.2851562, -0.0791626, -2.7539062, -0.9707031, -0.36669922, -2.0605469,
                  -0.453125, 0.70751953, -5.1796875, 2.4394531, 0.88378906, -0.52001953, 0.89746094, 1.7773438,
                  0.14855957, -2.3535156, 2.3164062, -3.2265625, 1.3837891, -2.3789062, 0.75683594, 1.5947266,
                  -0.36035156, -1.8574219, 4.5820312, -1.46875, -0.6660156, 0.50146484, -0.5839844, -2.9433594,
                  0.18786621, -1.2363281, -1.8710938, -1.7216797, -0.32373047, 0.07171631, -1.2636719, -0.984375,
                  0.25683594, -1.1171875, 2.4003906, -0.08276367, -2.1757812, -0.45483398, -2.2890625, 0.6308594,
                  1.7480469, 0.16870117, -2.2109375, -1.1396484, 0.34106445, 0.65966797, -0.6254883, -0.8017578,
                  -0.12261963, -0.03894043, -0.8222656, -0.56152344, -2.7421875, 2.0996094, 2.3671875, 4.984375,
                  0.06347656, 1.2265625, 1.1142578, 1.8261719, 1.8486328, 0.2722168, -0.29296875, 1.5878906, 0.3251953,
                  0.7167969, -1.1601562, -0.38745117, 1.7392578, 3.1015625, -1.8261719, 0.9433594, 2.0585938,
                  -0.9394531, 0.59033203, 0.22314453, 0.9711914, -2.1269531, 1.2666016, -0.90185547, -2.5078125,
                  1.8105469, -0.018554688, -0.12866211, 1.7197266, 1.2353516, 0.17626953, 0.13427734, 1.140625,
                  0.69677734, -1.1845703, 1.2460938, 0.78564453, 2.5273438, -2.0996094, -0.99316406, 0.8144531,
                  0.3828125, 0.20568848, 1.4541016, 5.5585938, -0.2824707, -1.0976562, 1.3632812, 0.032836914,
                  -0.10784912, 0.6201172, -0.50683594, 0.90527344, 2.3847656, -0.3642578, -1.2724609, 0.38452148,
                  -0.65234375, 0.49047852, 0.19360352, 0.39282227, 0.21984863, 0.49731445, 0.70410156, 0.60595703,
                  1.2607422, 0.6464844, 0.5527344, 2.5859375, 0.7011719, 1.5410156, -0.5839844, 1.6767578, -0.31347656,
                  -1.9462891, 1.0576172, -0.9272461, 0.71972656, 0.3894043, 0.076416016, 0.15246582, 4.2265625,
                  -0.032409668, 0.7167969, -2.84375, 0.8779297, 1.2714844, 0.14978027, -1.8574219, 2.6640625, 2.2285156,
                  0.6269531, -0.075805664, 1.1191406, 0.9663086, 1.8925781, 0.10656738, -0.6035156, -2.625, 0.57958984,
                  -2.0546875, 1.0654297, -0.18054199, -0.12915039, 0.5996094, -1.1923828, 1.1132812, 1.2822266,
                  4.6015625, -0.38842773, -2.5761719, 3.1269531, 0.89941406, 1.3701172, -1.5400391, 1.3378906,
                  0.9897461, 0.053222656, 1.2158203, -0.33325195, -0.828125, 1.5341797, 0.7158203, -3.2558594,
                  0.4753418, -0.7519531, 0.2529297, 1.6210938, 0.6645508, -2.1289062, 0.50683594, -1.2480469, 1.1738281,
                  1.1523438, -1.4677734, -0.12042236, 0.07397461, -0.09057617, -1.3017578, -1.1767578, 0.94873047,
                  1.0585938, -0.1517334, 0.5336914, -0.25878906, 0.90283203, -0.7426758, -0.020355225, 1.2871094,
                  -1.0507812, 0.116882324, -1.1318359, 0.35253906, 1.4345703, -0.122802734, -0.60791016, -0.05404663,
                  -0.83447266, 0.7636719, 1.2470703, -1.4765625, -0.7089844, -2.1308594, -1.4423828, 1.4726562,
                  -0.7553711, -0.78515625, -0.27392578, 0.29785156, -1.7734375, 0.0826416, -0.2536621, 0.68310547,
                  -0.9609375, -3.1679688, 1.5800781, 1.0292969, 2.3808594, 0.46313477, 1.0039062, -0.56396484, 0.734375,
                  0.95751953, 0.44091797, 0.8618164, -0.76171875, -0.49047852, -0.54541016, -0.3466797, 0.6430664,
                  -1.6503906, 0.03326416, -2.1445312, 5.0898438, -0.053894043, 0.44873047, 0.7597656, -0.35473633,
                  -0.5566406, -1.4902344, -0.6166992, 1.4433594, -1.1455078, 0.066467285, 0.5473633, 1.4306641,
                  1.1386719, -1.0332031, 0.7807617, 1.4501953, -2.9785156, -2.3867188, 0.6796875, 1.2783203, -2.2617188,
                  -0.86328125, 0.38110352, 1.8085938, 0.17102051, 0.38671875, -2.8398438, 2.5117188, -0.5917969,
                  0.47998047, 1.4462891, 0.21435547, 1.9931641, 0.609375, -0.39746094, 0.70410156, 0.36108398,
                  -1.9599609, -0.96875, -0.49902344, -0.26245117, -0.44921875, -0.75341797, 1.2617188, 0.9628906,
                  -0.87158203, 2.359375, 2.1582031, 0.71484375, -1.4980469, -1.3271484, -0.16040039, -0.50683594,
                  -2.0703125, 1.0019531, 0.8310547, 0.8496094, 1.9775391, 1.6962891, -0.9550781, -1.6621094, 0.2565918,
                  0.95751953, 0.95751953, 0.2298584, -2.3007812, 1.4921875, 0.9433594, -1.4648438, -0.7626953,
                  -1.8017578, -0.4152832, 0.35766602, -1.828125, 1.7949219, 1.8867188, -0.6328125, -0.99658203,
                  -1.6943359, 2.3203125, 0.5258789, -2.3046875, 0.009277344, 0.3449707, 2.1679688, -3.6171875,
                  -0.09539795, 0.81689453, 1.5693359, 0.47485352, -0.2548828, -0.65625, -0.98779297, -0.5292969,
                  -0.5292969, 2.7734375, -1.5039062, 0.7729492, 0.7675781, -1.8242188, 0.62060547, 0.19714355,
                  -1.0214844, 0.81640625, 1.1845703, -0.21984863, -1.3486328, 0.9980469, 3.046875, -0.10620117,
                  0.18054199, -3.4414062, -2.0097656, -0.24658203, -1.2998047, -0.07080078, -0.7714844, 1.3349609,
                  2.6738281, -0.9316406, -0.9116211, -1.0644531, -0.31420898, -0.9165039, 0.3256836, -0.80322266,
                  0.8852539, -0.29345703, 0.7558594, 0.99121094, 2.0507812, -2.0019531, -1.875, 1.0556641, -0.080322266,
                  0.3330078, 1.6132812, 1.1259766, -2.5195312, 0.7109375, -1.7988281, -1.1679688, -0.5058594,
                  -0.03845215, 0.47558594, 1.265625, -1.0595703, 1.2304688, -2.78125, 1.5849609, -1.6259766, 1.9443359,
                  -1.1113281, 0.8857422, 0.015045166, -1.9101562, -0.75097656, 0.11553955, -0.84521484, -1.4492188,
                  -1.2695312, -3.6484375, -0.4465332, -1.2050781, 1.6416016, -0.79345703, -0.5258789, 0.27929688,
                  0.12561035, -0.3876953, 0.2163086, -1.6201172, 1.4316406, -0.16259766, 0.029571533, 2.0800781,
                  -0.82128906, 1.1210938, 1.2587891, 1.1201172, 0.6645508, -0.5439453, 1.8056641, -1.6914062,
                  -0.57666016, -0.42407227, -0.89160156, 1.0068359, 0.9086914, -1.2978516, 1.4335938, -4.0234375,
                  0.29663086, -0.7055664, -1.2695312, -1.0273438, -0.32617188, -1.7851562, -2.0800781, 0.3190918,
                  -0.20983887, -1.6445312, 0.64208984, -1.2539062, -0.32763672, 0.9321289, -0.49560547, -0.5288086,
                  3.0996094, -0.11743164, -0.3605957, 1.7236328, 1.0175781, 0.94873047, -0.24072266, -0.2631836,
                  -0.43798828, 0.43701172, 1.3798828, -1.6962891, -1.5458984, -0.45263672, -0.8730469, 0.55126953,
                  1.7578125, -0.4675293, -1.0117188, 0.44970703, 3.3144531, 0.32055664, -1.6367188, 3.7089844,
                  0.7636719, 2.5683594, -0.13256836, -0.04159546, -1.9775391, 0.34155273, -1.0351562, 1.1835938,
                  0.8413086, 0.1394043, 0.06137085, -0.7714844, 0.3503418, -1.109375, -0.41088867, -0.26049805,
                  -0.64501953, 0.051940918, 2.578125, -3.9765625, -0.9296875, -1.5849609, -0.4248047, 4.7070312,
                  -2.1328125, 0.921875, -0.25048828, 0.8959961, 0.079956055, 0.1977539, -0.95410156, 0.5957031,
                  -1.3398438, -1.3789062, -0.7416992, 1.6650391, -1.3886719, -0.8388672, -0.8457031, 0.8222656,
                  2.0546875, 0.7495117, 1.6904297, -0.5253906, -0.7939453, 0.2006836, 2.3359375, -0.3256836, -1.0576172,
                  0.8256836, -1.3242188, 1.8417969, 0.6064453, 2.3320312, -0.0040245056, -0.7080078, -0.77441406,
                  -1.4160156, -1.7138672, 1.5058594, 2.0546875, 0.9550781, -0.921875, -1.4931641, 0.5605469,
                  -0.10760498, 1.2119141, 0.1381836, -0.5678711, -0.6689453, 0.9008789, 2.1054688, -0.28027344,
                  -2.2929688, -0.7817383, 1.4619141, 0.29785156, 2.5644531, 0.49438477, 0.78125, -1.1816406, 2.8398438,
                  -1.359375, 1.0478516, -2.6191406, -0.5253906, -0.8647461, -0.91845703, -2.5332031, 0.43896484,
                  -0.32299805, -0.5371094, 0.3701172, 1.015625, 0.92871094, 0.79248047, -2.5, 1.6601562, 0.54345703,
                  1.515625, -0.8027344, -0.095581055, 1.0703125, 1.7275391, 1.3476562, -2.3925781, -0.9741211,
                  -0.74609375, 5.1796875, -1.4111328, 2.53125, -0.5932617, 0.6196289, -0.36499023, -2.0703125,
                  -0.36816406, -0.5996094, -0.6621094, 0.53759766, -1.90625, -1.6054688, 1.46875, -1.0986328,
                  0.77441406, 0.31054688, -0.3322754, 1.9082031, 0.37646484, 2.0390625, -0.8925781, -0.95996094,
                  0.90185547, -0.78271484, 0.1661377, 1.9824219, 0.37548828, 1.6894531, -0.012992859, 0.6269531,
                  -2.8828125, -0.8886719, -0.20593262, -1.078125, 0.10864258, 0.5888672, -0.41137695, 0.8183594,
                  -0.4194336, 0.89501953, 0.8510742, -1.6328125, 1.0283203, -0.20898438, 3.4140625, 0.7441406,
                  0.36132812, -0.011932373, -0.42700195, 2.7539062, -1.84375, 0.025131226, -1.8486328, 0.7895508,
                  -5.4179688, -1.2402344, 1.2070312, -0.7158203, -1.2226562, -2.140625, 0.26757812, -0.5498047,
                  3.4609375, 0.7504883, -0.14672852, 0.56152344, 1.0507812, 1.5664062, 1.46875, 2.1816406, -0.58691406,
                  2.3398438, 1.2080078, 1.3242188, -0.7529297, 1.4238281, -0.23095703, -0.08190918, 0.5180664,
                  2.7207031, -0.16540527, -0.5708008, 0.13098145, 2.6015625, -0.69921875, 0.025421143, -0.43041992,
                  0.53271484, 1.1181641, 1.9394531, -0.18078613, -1.4433594, 3.0449219, 1.5, 1.2529297, -0.14978027,
                  -1.2431641, 0.20446777, 0.48046875, 0.012481689, -0.25830078, 1.3710938, 0.5576172, -0.8378906,
                  -2.0273438, -0.07043457, -0.42407227, -0.2163086, 0.6015625, 0.2220459, 1.2978516, -1.4033203,
                  1.1845703, 1.5507812, -1.921875, -1.546875, -1.1914062, 0.63378906, -0.99121094, -0.5839844,
                  -2.3398438, -1.0, -0.5629883, 1.0498047, -0.7480469, 0.6582031, -0.4765625, -3.171875, -1.6875,
                  1.4853516, -1.0224609, -0.9086914, -1.6923828, -1.4951172, -0.81152344, -1.6162109, -2.0625,
                  -1.6455078, 0.27441406, 0.9946289, 0.95996094, -0.9296875, -1.0019531, -0.023269653, -0.84228516,
                  0.030593872, 0.8457031, 3.1953125, 2.125, -1.8076172, -2.3945312, -0.8959961, 1.2138672, -2.3417969,
                  -0.7553711, -0.27807617, -0.7841797, -0.5625, -1.5712891, -0.7504883, 1.2285156, -1.1074219,
                  -1.8369141, -0.5004883, 1.5683594, 4.1054688, 0.65771484, -1.7714844, -0.2163086, -1.8232422,
                  1.0556641, 1.2685547, 1.8994141, -1.1679688, -0.73535156, 0.080322266, 2.2265625, -0.5722656,
                  1.2128906, 0.79248047, 1.7382812, 3.1640625, 0.39135742, -0.86865234, -0.4189453, -2.6972656,
                  -1.8574219, -0.3347168, 1.6816406, -1.9326172, -0.20495605, -0.44433594, -3.203125, -1.3125,
                  -1.4101562, -1.2851562, 1.984375, -2.0273438, -0.89160156, 0.68115234, 2.6171875, -0.36328125,
                  -0.3605957, -0.5175781, 0.71191406, -1.5136719, 0.32714844, 1.2148438, -0.8671875, -2.1386719,
                  0.60595703, 0.34936523, -1.6630859, 2.3476562, -3.2480469, 0.48266602, -1.0244141, 1.0693359,
                  2.1894531, -2.4941406, -1.3417969, -1.2841797, 1.7070312, -0.47045898, 1.3613281, 2.4023438,
                  -3.3457031, 0.6098633, 0.1295166, 0.50683594, -0.005180359, 1.6796875, 0.62060547, -0.22607422,
                  0.5175781, -1.9023438, -2.0703125, 1.3447266, -1.0800781, -1.0351562, -1.0771484, -0.62158203,
                  -1.3330078, -0.59375, -1.5703125, 0.42211914, -0.04849243, -1.9794922, 0.65966797, -3.1777344,
                  1.859375, -0.10308838, -0.86279297, 0.8652344, 1.8476562, -0.9321289, -0.7841797, -0.98876953,
                  2.1328125, 0.84375, -0.13305664, 5.6679688, -0.53222656, 0.33642578, -1.3095703, -2.0527344,
                  0.49047852, 1.5488281, 2.8691406, -0.21655273, -0.40625, 0.87890625, -0.46972656, -0.9296875,
                  -0.06994629, -2.2128906, -0.7734375, 1.4560547, 1.0, -0.18054199, 0.6088867, -0.026916504, 0.40600586,
                  2.25, -0.55371094, 0.66748047, 0.24609375, 0.81591797, -2.2578125, -0.5708008, -5.7578125,
                  -0.040222168, 1.3759766, 0.7680664, 0.80371094, 0.6464844, 2.625, -0.5078125, -1.5244141, -0.734375,
                  0.09893799, 1.5166016, 1.1914062, -3.4042969, 1.4404297, 2.5371094, 1.4492188, 0.6513672, 0.1171875,
                  0.3605957, -0.5253906, -0.3190918, -0.31274414, 1.6738281, -0.23547363, -0.5136719, 0.20141602,
                  2.3671875, 0.20080566, 1.3603516, 1.0361328, -1.3056641, 0.18969727, -0.7504883, -0.30395508,
                  -0.80810547, -0.97998047, -0.19189453, -2.5292969, 1.1679688, -2.1230469, -0.9863281, -0.041748047,
                  1.2324219, 0.84765625, 1.7392578, -1.3681641, 0.9550781, -2.0253906, -0.5888672, 0.37768555,
                  -0.99658203, -3.2695312, 0.5253906, -1.1210938, -0.56689453, -1.4990234, 0.28222656, 2.7089844,
                  -1.3876953, 1.5507812, 0.35327148, 1.5537109, 0.47705078, -1.1660156, -0.26708984, -1.6064453, 2.0,
                  -0.4404297, -0.30004883, 0.43237305, 0.22790527, -0.63134766, -1.7060547, -3.4453125, -2.1582031,
                  0.87402344, 0.85791016, -1.5097656, -0.09674072, 2.1875, 0.59521484, 0.07745361, -0.44799805,
                  -2.0488281, 0.92089844, -1.0625, 2.4941406, 2.4550781, 4.3242188, 0.40039062, -0.6933594, 1.9101562,
                  0.3408203, 1.1650391, -1.8388672, -2.1953125, -0.39794922, -1.5839844, -1.9931641, 1.4755859,
                  0.46972656, 1.109375, 0.7832031, 0.10180664, 2.046875, 1.4150391, 1.4013672, 1.0751953, 0.63720703,
                  -0.28759766, 0.8461914, -1.3525391, 0.30981445, -1.5605469, 0.27246094, -0.36547852, 0.12042236,
                  0.20227051, -0.29638672, -1.4443359, -0.37817383, 0.22570801, -1.7773438, 1.0488281, -0.2956543,
                  2.390625, -2.296875, 0.14196777, 0.70751953, -0.4658203, 0.047058105, -0.5888672, 0.83984375,
                  -0.95996094, 1.1044922, 0.70947266, 0.5722656, -2.1816406, -1.7617188, -0.49609375, -0.10870361,
                  0.8046875, -0.03781128, 2.0546875, 0.66552734, -1.2470703, 0.515625, -1.1298828, -0.45263672,
                  0.06964111, 0.20129395, 0.5361328, 2.3027344, 0.8413086, 0.8588867, 0.9321289, 0.23522949, 1.2558594,
                  0.28295898, 0.09484863, 1.5810547, -1.1191406, 2.3046875, 0.1665039, 2.9160156, -1.890625, -1.9375,
                  -2.6914062, -0.265625, 0.059906006, 0.64941406, -0.7416992, -1.5478516, 0.13305664, 1.2871094,
                  -1.2177734, -1.5527344, 0.22265625, 0.5908203, 0.0859375, 0.6665039, 1.4570312, 1.671875, -0.3635254,
                  -1.5712891, -1.0195312, 0.5932617, 1.0976562, -0.39013672, 2.0195312, 0.29492188, -0.4831543,
                  -1.1337891, -0.7036133, -2.0449219, -0.18408203, -1.1640625, 0.05505371, -0.9711914, -0.52734375,
                  0.45141602, -1.1748047, 1.5507812, 1.2363281, -1.5673828, -0.98339844, -0.15454102, -1.2382812,
                  -1.2910156, -0.47924805, -1.2841797, -0.7114258, -1.1064453, 2.8515625, 1.6767578, 1.921875,
                  0.8984375, -0.66748047, 1.3945312, -0.94873047, -0.9633789, 2.8066406, -0.38427734, -1.1582031,
                  -0.93310547, 1.3212891, -0.027435303, -0.1665039, 0.40698242, 0.7832031, 0.5234375, -1.1679688,
                  -1.3388672, 0.76464844, -0.73339844, 0.14208984, 1.6445312, 0.06365967, -0.73876953, -1.2685547,
                  0.8964844, 1.0117188, 2.0234375, 1.9726562, 3.6171875, 1.0800781, -0.9785156, 3.4453125, 1.2246094,
                  0.97558594, 0.048553467, -0.18945312, -1.4931641, -0.58740234, 0.7558594, 1.8681641, 1.1083984,
                  -1.4921875, -1.0400391, 0.8378906, -0.37060547, 0.65966797, -0.0949707, -1.0761719, -1.8232422,
                  -0.2770996, -1.1240234, 0.58154297, 0.014595032, -1.1630859, 1.9638672, 0.20666504, 0.13171387,
                  -0.85498047, 0.06762695, 1.3339844, -2.9667969, 0.09222412, 0.96191406, 1.1005859, -1.5664062,
                  1.8662109, -2.5449219, -0.17016602, -1.4013672, -0.48339844, 2.1289062, 0.3564453, -1.4365234,
                  -2.6894531, 1.2285156, 0.9824219, -1.5712891, -1.4248047, -1.7392578, -2.3925781, -1.3417969,
                  -1.2910156, -0.2836914, -1.0556641, -1.0527344, 0.1784668, -1.2148438, -0.17907715, 0.3630371,
                  -3.0292969, 1.8242188, 0.2529297, -0.24609375, 0.12432861, -0.40893555, 2.1015625, -0.20898438,
                  -0.94873047, 1.4414062, 1.1132812, -0.7861328, -0.7285156, -1.7539062, 0.2746582, 0.6616211,
                  -1.5107422, 1.0400391, 1.0703125, 1.4257812, -1.3457031, 1.4550781, 0.15991211, 1.9707031, 1.7529297,
                  -1.3681641, -0.29418945, -1.5019531, 0.31958008, 0.4892578, 1.1728516, -0.62353516, 0.16259766,
                  -0.58447266, 0.7324219, -0.9277344, -4.1914062, -0.86816406, -1.7392578, 0.7128906, 1.8554688,
                  -1.7685547, -1.5517578, 0.7910156, 1.0, -0.88427734, 1.2285156, -2.9179688, 0.4326172, -0.92529297,
                  -0.75634766, -0.83251953, 1.5, -0.32885742, 1.1611328, -0.90625, -0.07635498, -1.8173828, -0.5019531,
                  0.047973633, -1.2148438, -1.5029297, -0.33325195, 1.0546875, -0.84765625, -0.9633789, -2.4902344,
                  -1.9794922, -1.7675781, -0.8496094, 0.18835449, 0.46313477, 0.6308594, -1.9462891, -0.7446289,
                  -0.011528015, 0.6904297, -1.4726562, -0.87402344, 2.3710938, 1.453125, -0.29125977, 0.4873047,
                  0.3659668, 3.8203125, 0.56884766, 1.0908203, 0.27441406, 0.7089844, -0.03024292, -2.6503906,
                  1.5839844, 2.5878906, 4.21875, 1.2402344, -2.4765625, -1.6201172, -3.4453125, 1.4111328, 0.94970703,
                  2.1152344, 1.4111328, -1.6523438, -0.7963867, 0.088134766, 0.0690918, -0.19165039, 0.4921875,
                  -1.2578125, -0.5800781, -1.4697266, 1.0986328, 0.92529297, 0.9941406, 0.5800781, -0.0054244995,
                  0.39697266, 0.46264648, -1.6494141, -1.9150391, 0.5830078, -0.72802734, 1.2832031, 3.078125,
                  -0.28198242, -0.94628906, 5.2773438, 1.0039062, 0.96875, 0.7788086, 1.7539062, 0.1977539, 0.5991211,
                  -1.0693359, 0.18334961, 1.7089844, 0.4194336, -0.13598633, -0.58154297, -1.0117188, 0.5258789,
                  -0.42504883, -1.5908203, 2.359375, 0.49169922, -0.41137695, -0.004096985, -0.084350586, -2.1875,
                  0.07873535, 0.66552734, 3.0078125, 1.0986328, 2.0859375, -1.453125, 0.6503906, -2.1933594,
                  -0.59033203, -0.22790527, 2.4238281, 1.2265625, -6.1289062, -1.6396484, -0.7001953, -1.4638672,
                  0.09741211, 0.16064453, 0.6191406, -1.8710938, -0.11328125, -0.8417969, -1.1240234, 1.9082031,
                  -1.1884766, -1.3671875, -0.9082031, 1.1699219, -1.0947266, -0.54248047, 1.0332031, 0.15563965,
                  0.48095703, 1.453125, 3.4335938, -1.3916016, -4.5195312, 1.3164062, -1.8105469, 0.8286133, -2.453125,
                  -0.9584961, -0.11480713, 1.1308594, -0.7338867, 0.0053977966, 1.0097656, 0.5283203, -2.2226562,
                  -1.5244141, 0.77490234, -0.47485352, 1.3173828, -0.68603516, -0.1361084, 0.19360352, 1.6933594,
                  1.0322266, 0.7758789, 1.5419922, -0.5473633, 0.97753906, -2.4042969, -0.091918945, 1.5615234,
                  1.4169922, 0.02458191, -0.052886963, 2.328125, 0.18896484, -0.6743164, 1.1279297, -2.2578125,
                  -0.4453125, -2.0703125, -1.8955078, -0.65966797, 1.6328125, 1.6201172, 0.81396484, -1.015625,
                  1.6572266, 1.3720703, 1.2460938, 0.1550293, -2.6640625, -0.9428711, -0.06390381, -1.8300781,
                  -0.9550781, 1.6015625, -0.30004883, 0.37939453, -0.47216797, 2.0527344, 0.7163086, 0.46655273,
                  0.6352539, -0.46313477, -1.2539062, -1.2773438, -2.2246094, 0.47094727, 0.36035156, 3.2910156,
                  -1.8320312, -0.6894531, 0.81152344, 2.0039062, -0.95410156, -0.10418701, -0.22460938, 1.9423828,
                  -0.5527344, -0.20227051, -0.7133789, -1.2949219, 1.3359375, 0.076538086, -0.7236328, -0.99902344,
                  -0.5024414, 0.1751709, 0.3623047, 1.5439453, 0.31152344, 1.2382812, 0.5708008, -0.78515625,
                  -3.0195312, 1.7119141, 0.4921875, 1.2216797, -1.0087891, -0.17626953, -0.0068092346, 0.5683594,
                  3.1601562, -1.4179688, 1.3466797, -0.09741211, 1.0263672, -1.0068359, -0.22436523, 3.5546875,
                  -1.0029297, -1.4492188, 0.4248047, -0.17919922, 0.8569336, -0.1895752, 0.9975586, 0.82177734,
                  -0.6694336, -0.58691406, -1.0898438, -0.546875, -3.1835938, 0.3486328, 0.59521484, -1.2734375,
                  -0.1081543, 0.72216797, 1.8076172, 1.2548828, -0.92626953, -0.19238281, 2.6269531, -0.6977539,
                  1.1083984, -0.35839844, -1.2021484, 1.1210938, 0.3544922, -0.8178711, -0.1829834, -1.6650391,
                  -0.2055664, 0.2397461, -0.4152832, -1.6152344, 1.0664062, -0.30517578, 1.125, 1.2265625, 0.21081543,
                  0.9394531, 0.15429688, 1.9775391, -1.0185547, 1.3232422, -1.5126953, 0.49951172, -0.27441406,
                  -0.33032227, 1.984375, 0.8852539, -2.21875, -0.61816406, 0.32470703, -0.08251953, 2.0273438,
                  1.9257812, -0.6484375, -0.56640625, 3.5546875, 1.6416016, -0.011199951, -0.90527344, -2.7207031,
                  0.6591797, 2.1953125, -0.6455078, -1.4804688, 2.2441406, 3.7578125, 0.4658203, 3.0039062, 0.09173584,
                  -0.29907227, -1.2304688, -0.9086914, 1.4160156, 2.2949219, 0.90966797, -0.009002686, 0.03326416,
                  2.0390625, -0.5917969, 0.45581055, -2.125, -0.3708496, -0.09387207, -1.671875, -0.24865723,
                  -0.70410156, 1.8417969, -3.6386719, 0.90527344, -0.5209961, 1.0361328, -2.0, 1.5527344, 1.2792969,
                  0.00957489, -0.018051147, 1.0419922, -1.9648438, -1.0537109, -1.1376953, 1.0800781, -1.0507812,
                  0.44799805, -2.6230469, -2.2421875, 1.8349609, 1.6347656, -3.4570312, 5.4240227e-05, 0.42285156,
                  2.1035156, -1.2548828, -0.052124023, -0.0099487305, -0.90966797, -0.6855469, -1.2998047, -0.6582031,
                  1.1074219, 0.18432617, -1.6083984, -0.8730469, 1.9882812, 2.7832031, 1.6777344, 1.0625, -1.8818359,
                  -1.6425781, 0.9243164, -1.5390625, 3.0175781, -0.07550049, 0.39746094, -1.5996094, 1.2050781,
                  0.97558594, 2.0273438, -0.39453125, -1.796875, 0.31567383, -1.84375, 1.9755859, 0.7626953,
                  -0.21044922, 0.45410156, 0.18908691, -1.5488281, 1.4804688, -0.07397461, 1.7226562, -0.9555664,
                  -0.8076172, 0.4411621, 3.1035156, 0.041503906, 0.69873047, 1.0429688, -1.6210938, -2.2382812,
                  2.953125, 3.3886719, -0.42285156, 0.18225098, 0.6875, 1.5371094, -0.07507324, -2.4296875, 1.1523438,
                  -0.8925781, -0.88964844, 0.97753906, -2.203125, 1.9882812, 3.3164062, 1.9023438, -0.015029907,
                  -1.4013672, -0.7919922, 0.5878906, -2.6015625, 0.59814453, 0.8925781, 1.4541016, -1.9726562,
                  0.095458984, 0.46484375, 0.7207031, 0.21875, 2.7109375, -0.42626953, 0.57421875, 1.0166016,
                  -2.2265625, -0.8618164, 1.6542969, 2.015625, -2.0820312, 0.6044922, 0.9941406, 1.5917969, 1.2431641,
                  2.0351562, 0.74853516, 1.0478516, 1.0810547, -1.2871094, -0.40454102, -0.5996094, -0.19384766,
                  0.46142578, -1.1982422, 0.2154541, -2.0234375, -0.021759033, 0.37402344, 1.5175781, 0.7739258,
                  -0.6533203, 0.2939453, -0.93652344, 0.20251465, -3.4609375, 2.4277344, 0.006034851, 2.5585938,
                  0.6542969, -0.10058594, -0.9199219, -1.0771484, -1.2324219, 1.2353516, 1.0380859, 2.1933594,
                  0.31958008, 0.20422363, -1.71875, 0.7729492, 2.6796875, -5.0195312, -1.6601562, 0.31103516,
                  -1.2910156, -0.5493164, -0.45825195, -1.0458984, -1.4414062, 1.4570312, -1.0810547, 0.13232422,
                  0.3798828, -0.39697266, 0.115234375, 0.044799805, -1.0048828, -1.4169922, -0.053710938, -1.0996094,
                  -3.1933594, -1.8798828, 0.26611328, 2.5703125, 4.0273438, -0.40527344, 0.9277344, -0.21813965,
                  -0.066833496, 1.0888672, -1.140625, 2.7675781, -0.30151367, 0.33032227, 0.85058594, 0.08514404,
                  -3.4238281, 0.54833984, 0.92626953, 0.11590576, 0.24072266, -5.7148438, -1.140625, 1.2841797,
                  -0.38452148, -0.6274414, -1.5253906, 1.3466797, -0.89941406, 0.8178711, -1.3330078, 0.20446777,
                  0.6713867, -2.5058594, -1.9042969, 1.8398438, 0.61279297, 0.41870117, 2.1679688, 2.4101562,
                  -1.9824219, 0.3552246, 0.41625977, 1.6582031, 1.6464844, -0.1743164, 0.546875, -0.93408203, 2.2285156,
                  -0.7416992, -0.19372559, -0.56689453, -0.88964844, -0.9633789, -0.3942871, -1.6894531, 0.34057617,
                  0.84765625, 1.2197266, 1.1005859, 0.22045898, 0.12866211, -0.7558594, 0.2475586, -1.1464844,
                  2.5136719, 1.6054688, 0.5209961, 0.8984375, 1.7099609, -0.101867676, -0.8569336, -0.4831543,
                  1.8945312, 0.9941406, -0.4128418, 0.7817383, 1.3476562, 0.36035156, 1.5625, -0.1665039, -0.2084961,
                  0.56347656, 1.5927734, 1.1503906, 0.76904297, 0.16564941, -0.34033203, -0.011230469, -1.1796875,
                  -1.0537109, 0.89160156, -0.96972656, -0.0063476562, 0.04458618, 1.8457031, 0.7402344, -1.0273438,
                  1.0966797, -2.5332031, 1.9550781, -0.97314453, 0.107543945, -0.24047852, -0.28979492, -1.6171875,
                  -0.13085938, -0.42504883, 0.74316406, 3.2539062, -2.3652344, 0.35717773, 0.08569336, 1.2148438,
                  1.2714844, -0.50927734, -0.38989258, 0.4013672, 0.38500977, -1.4707031, 0.5361328, -0.53808594,
                  1.0341797, 1.8457031, 0.70996094, -0.67871094, -1.1416016, -0.01663208, 1.3154297, -0.38183594,
                  -0.07269287, 0.5024414, 1.4228516, -1.2246094, -2.0214844, 0.23071289, 1.65625, -2.328125, -1.5234375,
                  1.234375, -1.1601562, 3.0566406, -2.2753906, 0.6225586, -0.069885254, -0.7919922, -0.20837402,
                  0.79589844, -0.24389648, 0.4580078, 3.109375, 0.2232666, -1.2255859, 0.031951904, 2.8515625, -2.09375,
                  -0.5683594, -1.2724609, -0.8598633, 1.3730469, 1.1591797, -2.4785156, -2.6113281, -0.20263672,
                  0.14550781, 0.10211182, -2.2910156, 3.6914062, -0.1739502, -1.2744141, 1.0878906, -0.6669922,
                  -1.0605469, 2.1445312, 0.22961426, -0.97216797, 0.9296875, -0.25854492, 0.5498047, 1.3769531, 0.125,
                  -0.9941406, 0.47436523, -0.7705078, -2.0839844, 0.6582031, -1.0605469, 2.7675781, 0.30249023,
                  -0.36816406, -0.6821289, 0.22436523, 1.6757812, 0.83984375, 0.38671875, 0.87402344, 1.2744141,
                  1.1513672, -0.20996094, -0.6640625, 0.037322998, 1.2080078, -0.9355469, 3.0390625, 2.7910156,
                  -0.98095703, 0.14831543, -1.7607422, -0.37402344, 2.9609375, -0.26342773, 0.3479004, 0.8808594,
                  2.6640625, -0.016815186, -0.11242676, -1.5205078, 1.1191406, -1.5644531, -1.1367188, -1.0742188,
                  2.5644531, -0.5102539, -2.0820312, -2.0039062, -0.22070312, 1.2929688, -0.8730469, -0.59375, -2.1875,
                  1.8193359, 1.1972656, -0.5703125, 0.33081055, -0.49731445, -0.75, 0.16223145, 0.1463623, -1.5019531,
                  3.1210938, -0.37109375, 1.9755859, -0.71875, 2.4863281, -1.4648438, -0.93310547, 0.37060547,
                  -0.14904785, 4.328125, -2.5136719, -1.1064453, -2.6210938, 0.74560547, 2.0351562, 3.0136719,
                  -1.0507812, 0.93310547, -1.6855469, -0.3046875, 2.6640625, 0.93847656, -0.6621094, -1.9951172,
                  -0.84033203, 0.31079102, 0.21948242, 0.62597656, -1.0751953, 1.5742188, -2.0722656, 0.5361328,
                  1.8837891, 2.0820312, -0.33203125, 1.8945312, 3.3574219, 1.5078125, -0.23046875, -0.9272461,
                  -0.6738281, -0.28637695, 1.7949219, -1.3183594, 0.4428711, -0.8413086, 0.27807617, 3.0117188,
                  3.1542969, -1.5146484, -0.6269531, 0.12219238, -0.13195801, 0.22485352, 0.8144531, -1.8789062,
                  1.0976562, -0.025665283, 1.3339844, 1.0556641, -1.9150391, 0.09503174, -0.50341797, -0.46704102,
                  0.6796875, -3.53125, -1.15625, -0.77783203, 1.3476562, 2.1113281, 1.5429688, -2.7539062, -0.21569824,
                  1.3779297, 0.22290039, 2.6582031, 0.5913086, 0.5019531, 0.89501953, -0.18359375, 1.0693359, 0.5966797,
                  -1.1835938, 1.2177734, 0.9160156, -1.0976562, -0.057556152, -1.2470703, -0.42163086, 1.2851562,
                  0.36523438, -0.047576904, -4.328125, 1.5048828, -0.32202148, -2.1445312, 0.72265625, -1.9443359,
                  -1.7724609, 0.7421875, 0.7636719, -1.5097656, -0.12634277, 0.5551758, 0.35107422, 1.4248047,
                  -0.5620117, -1.3632812, 1.4130859, 0.18457031, -0.98535156, 1.7246094, 2.59375, -1.1875, 0.3552246,
                  0.06713867, 0.8149414, -1.546875, 1.6630859, -1.4726562, 2.7578125, -0.79345703, -1.4169922,
                  -0.9765625, -2.8945312, 1.8554688, 3.3027344, -0.48339844, -1.234375, 0.9663086, 1.0429688,
                  -3.1289062, 0.19055176, 1.8417969, -0.8256836, -0.69433594, -1.0898438, -1.0273438, -3.1757812,
                  -0.94628906, -0.37280273, 1.6992188, 1.1591797, -0.01272583, -1.0664062, -0.1673584, 0.2841797,
                  -1.3369141, -1.1650391, -2.8554688, -0.09136963, -2.1230469, 0.04650879, 0.9741211, 0.5878906,
                  -0.6557617, -0.6279297, -0.9375, 0.83740234, -1.3554688, 1.1455078, 0.074523926, -0.8183594,
                  0.4091797, 1.2294922, 1.9707031, -0.8828125, -0.34692383, -1.15625, -0.45092773, 0.5517578,
                  -1.9013672, -0.12084961, 0.19799805, -0.7548828, 0.14941406, -1.3300781, 2.6972656, -1.2070312,
                  1.5048828, 1.0703125, 0.07476807, -0.97753906, -1.8193359, -0.043182373, 1.5966797, -0.3095703,
                  0.47558594, -0.08770752, -0.39892578, -1.9306641, 0.85058594, 1.4042969, 0.6801758, 2.3691406,
                  0.73779297, -0.38452148, -1.234375, -0.64941406, -0.48266602, -1.7949219, 1.5771484, -2.8847656,
                  -0.32495117, -0.093322754, -1.4072266, 1.8525391, 1.1552734, -1.3417969, 1.3671875, 0.5761719,
                  -0.24133301, 0.8286133, 0.49829102, -0.21740723, -0.9238281, -1.1523438, 0.049041748, -1.7539062,
                  -0.36987305, 0.015594482, 1.6796875, 0.79833984, 1.1318359, -1.7705078, -0.36157227, -1.6591797,
                  -0.82958984, 2.1777344, -0.9238281, 0.117492676, 1.1054688, 0.018249512, 2.1855469, 0.9609375,
                  -1.8476562, 0.013183594, 0.9589844, 0.53564453, 2.5195312, -0.2919922, 2.4921875, -0.49658203,
                  -0.22436523, 2.0644531, -0.41064453, -0.33935547, 4.5039062, -0.8925781, -0.8066406, -0.06713867,
                  0.03918457, -2.7148438, -0.46264648, -0.5175781, 0.39892578, -2.6582031, 2.2402344, -0.1171875,
                  0.72509766, 2.0117188, -0.13476562, 0.3413086, 3.5292969, 0.70751953, -0.6455078, 1.3867188,
                  -2.0234375, 1.8505859, 0.3251953, 1.4482422, -1.3007812, -1.6083984, 0.99121094, -0.2644043,
                  0.71191406, 0.19299316, 1.0224609, 1.8144531, 1.6914062, -1.1289062, 0.6665039, -0.62890625,
                  0.008140564, 0.6088867, 0.49047852, 2.2675781, 1.0429688, 0.19726562, 0.08886719, -1.984375,
                  -1.6279297, -0.8408203, 0.80810547, -0.56640625, -0.7167969, 0.17077637, 0.8457031, 2.3339844,
                  -0.47753906, -0.93408203, -0.89404297, -1.0791016, 3.4140625, -1.3808594, -1.0507812, -0.27246094,
                  0.10839844, -1.0253906, 1.2607422, 1.3085938, 0.1616211, 1.0195312, -0.6982422, 0.21374512,
                  -1.5458984, 0.45996094, 0.6772461, -2.2441406, -0.49902344, 0.6669922, -2.3164062, -0.9140625,
                  -0.3642578, -0.13696289, -0.20056152, -0.21862793, 0.9667969, -2.4082031, 0.06567383, -1.2802734,
                  1.7675781, 0.6669922, 1.2626953, -0.107055664, -1.8925781, -2.1035156, 4.0664062, 0.7939453,
                  -1.3037109, -0.75878906, 0.5332031, -1.3505859, 2.0703125, -2.6777344, -1.5595703, 2.8027344, 2.46875,
                  1.9257812, 1.1679688, -0.39379883, 2.2265625, 0.82421875, -1.7509766, -0.64941406, -1.7070312,
                  -1.1083984, -3.8925781, 0.6142578, -1.4794922, 2.765625, -1.1689453, -0.027267456, -2.3867188,
                  1.2314453, -2.4550781, 1.8056641, -1.59375, -0.5078125, 0.44970703, 0.41992188, 1.8857422, 0.6791992,
                  -1.0390625, 0.074401855, -0.5805664, 1.0947266, 2.3203125, -0.55859375, 0.62939453, 2.2597656,
                  2.5546875, 1.4384766, -3.2226562, 2.359375, 1.4960938, -0.3310547, -0.44799805, 0.7324219,
                  -0.51953125, -0.27416992, -2.4609375, -0.63671875, -2.1035156, 0.49169922, -4.9375, 1.3164062,
                  -0.56884766, -1.5732422, -1.8017578, 1.6923828, -0.26464844, 1.1503906, 1.9394531, -0.51123047,
                  -1.8945312, 0.81689453, 0.5073242, 1.8017578, -1.5703125, 0.3173828, 0.2602539, -3.6796875,
                  -1.6064453, -2.9628906, 0.69628906, -0.83447266, 0.5078125, -0.05126953, 0.9555664, -2.4570312,
                  -0.45214844, 2.0546875, -0.6796875, 0.64941406, 1.8681641, -1.7460938, -0.12646484, 0.40551758,
                  -0.8017578, 1.3632812, -0.49487305, 1.3085938, -1.1064453, -1.3671875, 0.47875977, 0.09680176,
                  0.72216797, -0.64208984, -3.1542969, 0.9375, -0.4375, -2.7734375, -2.1132812, 0.7753906, 2.5546875,
                  -0.8334961, -3.3828125, -0.7216797, 0.9243164, -2.3457031, -1.3242188, -0.14331055, -0.6743164,
                  0.5449219, 1.4804688, 0.5317383, -2.03125, 1.1025391, 0.3503418, -0.5683594, 0.17822266, 0.1694336,
                  1.0517578, -0.26660156, -1.3789062, 0.63183594, 0.22790527, 2.1992188, 0.13623047, -0.6147461,
                  2.3984375, 1.0507812, -2.1230469, 0.5527344, -0.3881836, -1.4892578, 1.6826172, -0.97753906,
                  0.21789551, -3.4902344, 1.4619141, 1.0585938, -2.0253906, 1.8017578, 2.0175781, 1.4941406, 1.6279297,
                  0.109802246, 2.4804688, -0.5761719, -2.4960938, -1.5244141, -1.0712891, 0.48388672, 0.5449219,
                  -0.85595703, -0.35375977, -0.80859375, 2.2714844, -0.72802734, -0.99316406, 1.6425781, -0.55078125,
                  0.47143555, -0.86816406, -0.7211914, -1.0507812, 1.9863281, 0.98876953, -0.35058594, 1.2988281,
                  1.3349609, 1.7460938, -0.76416016, 0.8256836, -1.7167969, -1.6972656, 0.37768555, -1.2753906,
                  -1.9775391, 1.2382812, -0.80615234, -1.6201172, 2.7363281, -1.1982422, 0.2614746, -1.4130859,
                  -0.77246094, -1.4482422, 2.6113281, -1.0361328, -0.8925781, 1.6474609, 1.3505859, 1.3710938,
                  -1.4550781, 0.9477539, -0.51220703, -0.8588867, 0.3701172, 0.12475586, -0.5439453, -0.8027344,
                  -0.15759277, 0.35839844, -0.14074707, -1.0683594, 0.94873047, 0.006511688, -0.28271484, -0.5996094,
                  1.1220703, -0.27783203, 2.7773438, -2.1152344, -0.6376953, 1.3476562, -0.84375, 0.70751953, 1.203125,
                  1.6630859, 0.9472656, -0.18896484, -0.31298828, 2.4785156, 1.390625, -0.7480469, -0.44628906,
                  0.62158203, 0.24023438, -0.3190918, -0.7167969, 0.34472656, -0.16809082, 0.25976562, 1.9755859,
                  -1.5449219, 0.7392578, 0.3034668, 1.1269531, -2.1972656, -1.1455078, -0.031921387, 2.3652344,
                  -1.1503906, 0.44628906, 0.059570312, -3.0234375, -2.015625, -0.3527832, -2.3691406, -2.5957031,
                  1.7207031, -0.33496094, 1.8681641, -1.4941406, 1.1435547, 0.97216797, -0.82666016, -0.23071289,
                  -0.0715332, -0.95654297, -1.859375, 0.56933594, 0.32299805, -0.5097656, 0.5957031, -0.9584961,
                  -0.12927246, -0.058807373, 1.6142578, 0.8027344, 1.3720703, -0.8720703, -1.3388672, 0.58691406,
                  1.4257812, -0.46484375, -0.119384766, 1.8994141, 4.1171875, 0.11968994, -0.77978516, 0.19787598,
                  -0.8149414, -2.5527344, 1.6425781, 0.112854004, 0.0793457, -0.42626953, 0.40527344, 0.93408203,
                  1.7275391, -0.72216797, -0.7006836, 0.203125, 0.5708008, -2.5390625, 2.7070312, -0.84521484,
                  -0.5527344, -0.37597656, -0.7089844, -0.5566406, 0.24975586, -0.19580078, 0.3395996, 1.6318359,
                  -0.93408203, 0.30151367, 1.0917969, 0.62939453, -0.0043411255, -1.7509766, 0.11608887, 1.8525391,
                  -2.8203125, 1.5595703, -2.4257812, 1.0898438, 0.25195312, -0.62060547, -1.7480469, 4.4609375,
                  0.42504883, 2.1171875, 1.6230469, 0.28295898, 0.65283203, 1.0966797, -0.85009766, 2.7109375,
                  -0.5996094, -0.63378906, 0.78808594, -0.96533203, 0.54345703, 2.9023438, 0.87353516, 0.54345703,
                  0.066101074, -0.15258789, -1.4833984, -3.875, -0.95214844, 0.66845703, -0.7128906, 1.0986328,
                  -1.0224609, -1.1777344, -0.30151367, 1.7626953, -3.5078125, 0.25317383, 1.0585938, 0.4411621,
                  -1.4931641, -0.64697266, -1.1494141, 0.19372559, -1.5263672, 0.1763916, -1.1855469, 0.7089844,
                  0.036315918, 0.5336914, 1.3398438, -2.0429688, -0.25463867, -1.3349609, -0.02067566, -1.1728516,
                  -1.6708984, 1.2265625, 0.5649414, 1.4589844, 3.2402344, 0.13305664, 1.8554688, -0.07086182,
                  -0.45654297, -0.33496094, 0.07171631, 1.6826172, 1.1650391, -1.2958984, -1.5029297, 0.73779297,
                  -1.4736328, 0.8208008, 1.0517578, -0.61816406, 0.8227539, -0.50341797, 0.07159424, 1.3134766,
                  -0.21984863, -2.6171875, -0.33422852, 0.6796875, 1.0410156, -0.98095703, 1.2470703, -0.22070312,
                  1.3886719, -1.1601562, -1.3476562, -1.8193359, -4.1640625, -0.8847656, -2.1484375, -0.74853516,
                  0.17932129, -0.60839844, -0.12237549, 0.024627686, -3.1796875, 1.53125, -0.10601807, -0.9628906,
                  -0.8984375, -1.6054688, 1.5507812, -0.21313477, -0.29223633, 0.62597656, -0.9472656, -2.3828125,
                  -1.3447266, -0.5283203, -2.1738281, -0.87890625, -1.6806641, 2.9570312, 1.1425781, -1.1445312,
                  -1.6279297, 2.1796875, -1.9726562, -1.2597656, 1.7226562, 4.15625, -0.7167969, -0.25585938,
                  0.28881836, -0.47265625, 2.2148438, 0.77001953, 2.0058594, 4.1210938, 0.2775879, 1.8925781, -2.109375,
                  -0.050201416, 0.8569336, 0.3955078, -1.1982422, 1.5507812, 1.3945312, -0.42041016, -1.4814453,
                  -0.84228516, -1.7021484, 0.76708984, -0.79248047, -1.2548828, -1.8193359, -0.48999023, -0.9584961,
                  0.97753906, 0.4350586, -4.796875, 1.7929688, 2.4628906, -0.59716797, 2.2890625, -2.6367188, 3.3691406,
                  0.091918945, -2.234375, 1.6318359, -1.3945312, 2.4726562, 0.62353516, 0.6645508, 4.6171875, 1.7382812,
                  -0.87109375, 0.19482422, -0.22351074, -2.8808594, -0.453125, -0.94873047, -0.3564453, 0.003227234,
                  1.5966797, 1.0058594, 0.54003906, -0.22290039]
        vec_1 = f"ANN_DISTANCE(emb, {vector}, '{similarity}', {scan_nprobes})"

        # single vector field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'emb', index_fields=['emb VECTOR'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity,
                            scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"emb, {vec_1}",
                                                                              vec_1)))

        # Single vector + single scalar field + partitioned on meta().id
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'oneScalarLeadingOneVectorPart1',
                            index_fields=['name', 'emb VECTOR'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"name, emb,"
                                                                               f" {vec_1}",
                                                                               'name like "%Dil" ',
                                                                               vec_1
                                                                               ),
                            partition_by_fields=['meta().id']))

        # Single vector + single scalar field + partitioned on vector field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'oneScalarLeadingOneVectorPart2',
                            index_fields=['country', f'emb VECTOR'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"fuel, emb,"
                                                                               f" {vec_1}",
                                                                               'country = "Greece" ',
                                                                               vec_1
                                                                               ),
                            partition_by_fields=['emb']))

        # Single vector (leading) + single scalar field
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'oneScalarOneVectorLeading',
                            index_fields=[f'emb VECTOR', 'type'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"type, emb",
                                                                               'type in ["Inn", "Hostel"] ',
                                                                               vec_1)))

        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'multiScalarOneVector_1',

                            index_fields=['avg_rating', 'emb Vector', 'free_breakfast'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"avg_rating, free_breakfast,"
                                                                               f" emb, {vec_1}",
                                                                               "avg_rating > 3 and "
                                                                               "free_breakfast = true ",
                                                                               vec_1)))

        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'includeMissing',

                            index_fields=['country', 'emb Vector', 'avg_rating'],
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64, missing_indexes=True,
                            missing_field_desc=True,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"avg_rating, free_breakfast,"
                                                                               f" emb, {vec_1}",
                                                                               'avg_rating = 3 and '
                                                                               'country == "Greece"',
                                                                               vec_1)))

        # Partial Indexes
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'PartialIndex',
                            index_fields=['avg_rating ', 'country', f'emb Vector'],
                            index_where_clause='avg_rating > 2',
                            dimension=description_dimension, description=f"IVF,{quantization_algo_description_vector}",
                            similarity=similarity, scan_nprobes=scan_nprobes,
                            train_list=train_list, limit=limit, is_base64=is_base64,
                            query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"country, emb",
                                                                               "avg_rating = 4 and "
                                                                               'country like "%%ra%%" ',
                                                                               vec_1)))

        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_person_data_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "person" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'age', index_fields=['age'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", "age > 0")))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "suffix is not NULL"), is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'firstName_lastName', index_fields=['firstName', 'lastName'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'firstName like "%%D%%" AND '
                                                                      'LastName is not NULL')))

        # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys', index_fields=['age', 'city', 'country'],
                            missing_indexes=True, missing_field_desc=False,
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'firstName like "%%D%%" AND '
                                                                      'LastName is not NULL')))

        # Partitioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['streetAddress'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'streetAddress is not NULL'),
                            partition_by_fields=['streetAddress'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index', index_fields=['filler1'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'filler1 like "%%in%%"')))
        self.batch_size = len(definitions_list)
        return definitions_list

    def get_create_index_list(self, definition_list, namespace, defer_build_mix=False,
                              defer_build=False, num_replica=None, deploy_node_info=None, randomise_replica_count=False,
                              trainlist=None, dimension=None, description=None, similarity=None, scan_nprobes=None,
                              bhive_index=False):
        create_index_list = []
        for index_gen in definition_list:
            nodes_list = None
            if defer_build_mix:
                defer_build = random.choice([True, False])
            if deploy_node_info is not None:
                nodes_list = deploy_node_info
            if randomise_replica_count and num_replica >= 1:
                num_replicas = random.randint(1, num_replica)
                if deploy_node_info is not None:
                    nodes_list = random.sample(deploy_node_info, k=num_replicas + 1)
            else:
                num_replicas = num_replica
            query = index_gen.generate_index_create_query(namespace=namespace, defer_build=defer_build,
                                                          num_replica=num_replicas, deploy_node_info=nodes_list,
                                                          train_list=trainlist, dimension=dimension,
                                                          description=description, similarity=similarity,
                                                          scan_nprobes=scan_nprobes, bhive_index=bhive_index,
                                                          persist_full_vector=index_gen.persist_full_vector)
            create_index_list.append(query)
        return create_index_list

    def get_build_indexes_query(self, definition_list, namespace):
        index_name_list = [f"`{index_gen.index_name}`" for index_gen in definition_list]
        build_indexes_string = ", ".join(index_name_list)
        build_query = f'BUILD INDEX ON {namespace} ({build_indexes_string})'
        return build_query

    def get_drop_index_list(self, definition_list, namespace):
        drop_index_list = []
        for index_gen in definition_list:
            query = index_gen.generate_index_drop_query(namespace=namespace)
            drop_index_list.append(query)
        return drop_index_list

    def get_select_queries(self, definition_list, namespace, limit=0, index_name=None):

        select_query_list = []
        for index_gen in definition_list:
            if index_name:
                query = index_gen.generate_use_index_query(bucket=namespace, index_name=index_name)
            else:
                query = index_gen.generate_query(bucket=namespace)
            if index_gen.limit is not None:
                if limit == 0 and index_gen.limit > 0:
                    limit = index_gen.limit
            if limit > 0:
                query = f'{query} LIMIT {limit}'
            select_query_list.append(query)
        return select_query_list

    def get_count_query(self, dataset, namespace):
        if dataset == 'Person' or dataset == 'default':
            query = f"select * from {namespace} where age>0"
        elif dataset == 'Employee':
            query = f"select * from {namespace} where name is not null"
        elif dataset == 'Hotel':
            query = f"select * from {namespace} where price>0"
        elif dataset == 'Magma':
            query = f"select * from {namespace} where name is not null"
        else:
            raise Exception("Provide correct dataset. Valid arguments are Person, Employee, Magma, and Hotel")
        return query

    def async_create_indexes(self, create_queries, database=None, capella_run=False, query_node=None):
        with ThreadPoolExecutor() as executor:
            tasks = []
            for query in create_queries:
                if capella_run:
                    task = executor.submit(self.run_query, database=database, query=query)
                else:
                    task = executor.submit(self.run_query, query=query, server=query_node)
                tasks.append(task)
        return tasks

    def create_gsi_indexes(self, create_queries, database=None, capella_run=False, query_node=None):
        tasks = []
        with ThreadPoolExecutor() as executor:
            for query in create_queries:
                if capella_run:
                    tasks.append(executor.submit(self.run_query, database=database, query=query))
                else:
                    tasks.append(executor.submit(self.run_query, query=query, server=query_node))
            try:
                for task in tasks:
                    task.result()
            except Exception as err:
                print(err)

    def aysnc_run_select_queries(self, select_queries, database=None, capella_run=False, query_node=False,
                                 scan_consistency=None):
        with ThreadPoolExecutor() as executor:
            tasks = []
            for query in select_queries:
                if capella_run:
                    task = executor.submit(self.run_query, database=database, query=query,
                                           scan_consistency=scan_consistency)
                else:
                    task = executor.submit(self.run_query, query=query, server=query_node,
                                           scan_consistency=scan_consistency)
                tasks.append(task)
        return tasks

    def run_continous_query_load(self, select_queries, database=None, capella_run=False,
                                 query_node=False, sleep_timer=30):
        while self.query_event.is_set():
            try:
                tasks = self.aysnc_run_select_queries(select_queries=select_queries, database=database,
                                                      capella_run=capella_run, query_node=query_node)
                for task in tasks:
                    task.result()
            except Exception as err:
                self.log.error(f"Error occurred during query load: {err}")
            time.sleep(sleep_timer)

    def range_unequal_distribution(self, number=4, factor=1.2, total=100000):
        """
        This method divides a range into unequal parts of given number
        """
        z = total * (1 - 1 / factor) / (factor ** number - 1)
        distribution_list = []
        for i in range(number):
            part = round(z * factor ** i)
            distribution_list.append(part)
        distribution_list[number - 1] = total - reduce(lambda x, y: x + y, distribution_list[:number - 1])
        return distribution_list

    def index_operations_during_phases(self, namespaces, database=None, dataset='Employee', num_of_batches=1,
                                       defer_build_mix=False, phase='before', capella_run=False, query_node=None,
                                       batch_offset=0, timeout=1500, query_weight=1):
        if phase == 'before':
            self.create_indexes_in_batches(namespaces=namespaces, database=database,
                                           dataset=dataset, num_of_batches=num_of_batches,
                                           defer_build_mix=defer_build_mix, capella_run=capella_run,
                                           query_node=query_node, batch_offset=batch_offset)
        elif phase == 'during':
            self.run_query_workload_in_batches(database=database, namespaces=namespaces, capella_run=capella_run,
                                               query_node=query_node, timeout=timeout, query_weight=query_weight)
        elif phase == "after":
            self.cleanup_operations()

    def create_indexes_in_batches(self, namespaces, database=None, dataset='Employee', num_of_batches=1,
                                  defer_build_mix=False, capella_run=False, query_node=None,
                                  batch_offset=0):
        self.initial_index_num, create_list = 0, []
        for item in range(num_of_batches):
            for namespace in namespaces:
                counter = batch_offset + item
                prefix = f'idx_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}_batch_{counter}_'
                self.definition_list = self.get_index_definition_list(dataset=dataset, prefix=prefix)
                create_list = self.get_create_index_list(definition_list=self.definition_list, namespace=namespace,
                                                         defer_build_mix=defer_build_mix)
                self.log.info(f"Create index list: {create_list}")
                self.initial_index_num += len(create_list)
                self.create_gsi_indexes(create_queries=create_list, database=database,
                                        capella_run=capella_run, query_node=query_node)

        # results = self.run_query(database=database, query="select * from system:indexes")
        # self.log.info(f"system:indexes after create_indexes_in_batches is complete: {results}")

    def run_query_workload_in_batches(self, database, namespaces, capella_run, query_node, timeout, query_weight):
        start_time = datetime.datetime.now()
        select_queries = []
        while len(select_queries) < 100:
            for namespace in namespaces:
                select_queries.extend(
                    self.get_select_queries(definition_list=self.definition_list, namespace=namespace))
            select_queries = select_queries * query_weight
        with ThreadPoolExecutor() as executor:
            while True:
                tasks = []
                for query in select_queries:
                    if capella_run:
                        task = executor.submit(self.run_query, database=database, query=query)
                    else:
                        task = executor.submit(self.run_query, query=query, server=query_node)
                    tasks.append(task)

                for task in tasks:
                    task.result()
                curr_time = datetime.datetime.now()
                if (curr_time - start_time).total_seconds() > timeout:
                    break

    def cleanup_operations(self):
        pass

    def get_index_definition_list(self, dataset, prefix=None, skip_primary=False, similarity="L2", train_list=None,
                                  scan_nprobes=1, array_indexes=False, limit=None, quantization_algo_color_vector=None,
                                  quantization_algo_description_vector=None, is_base64=False, bhive_index=False,
                                  xattr_indexes=False, description_dimension=384, persist_full_vector=True, scalar=False):
        if dataset == 'Person' or dataset == 'default':
            definition_list = self.generate_person_data_index_definition(index_name_prefix=prefix,
                                                                         skip_primary=skip_primary)
        elif dataset == 'Employee':
            definition_list = self.generate_employee_data_index_definition(index_name_prefix=prefix,
                                                                           skip_primary=skip_primary)
        elif dataset == 'Hotel':
            if description_dimension == 4096:
                definition_list = self.generate_hotel_data_index_definition_vectors(index_name_prefix=prefix, scan_nprobes=scan_nprobes, similarity=similarity, limit=limit, quantization_algo_description_vector=quantization_algo_description_vector, description_dimension=description_dimension)
            else:
                definition_list = self.generate_hotel_data_index_definition(index_name_prefix=prefix,
                                                                        skip_primary=skip_primary)
        elif dataset == 'Magma':
            definition_list = self.generate_magma_doc_loader_index_definition(index_name_prefix=prefix,
                                                                              skip_primary=skip_primary)
        elif dataset == "siftBigANN":
            definition_list = self.generate_shoe_vector_index_defintion_bhive(index_name_prefix=prefix,
                                                                              skip_primary=skip_primary,
                                                                              similarity=similarity,
                                                                              train_list=train_list,
                                                                              scan_nprobes=scan_nprobes,
                                                                              limit=limit,
                                                                              persist_full_vector=persist_full_vector,
                                                                              quantization_algo_description_vector=quantization_algo_description_vector)
        elif dataset == 'Cars':
            self.log.info(f"scalar is {scalar}")
            if scalar:
                definition_list = self.generate_car_data_index_definition_scalar(index_name_prefix=prefix,
                                                                            skip_primary=skip_primary)
            elif bhive_index:
                definition_list = self.generate_car_vector_loader_index_definition_bhive(index_name_prefix=prefix,
                                                                                         skip_primary=skip_primary,
                                                                                         similarity=similarity,
                                                                                         train_list=train_list,
                                                                                         scan_nprobes=scan_nprobes,
                                                                                         array_indexes=array_indexes,
                                                                                         limit=limit,
                                                                                         xattr_indexes=xattr_indexes,
                                                                                         quantization_algo_color_vector=quantization_algo_color_vector,
                                                                                         quantization_algo_description_vector=quantization_algo_description_vector)
            else:
                definition_list = self.generate_car_vector_loader_index_definition(index_name_prefix=prefix,
                                                                                   skip_primary=skip_primary,
                                                                                   similarity=similarity,
                                                                                   train_list=train_list,
                                                                                   scan_nprobes=scan_nprobes,
                                                                                   array_indexes=array_indexes,
                                                                                   limit=limit, is_base64=is_base64,
                                                                                   xattr_indexes=xattr_indexes,
                                                                                   quantization_algo_color_vector=quantization_algo_color_vector,
                                                                                   quantization_algo_description_vector=quantization_algo_description_vector, description_dimension=description_dimension)
        elif dataset == 'MiniCar':
            definition_list = self.generate_mini_car_vector_index_definition(index_name_prefix=prefix,
                                                                             skip_primary=skip_primary)
        else:
            raise Exception("Provide correct dataset. Valid values are Person, Employee, Magma, Car, MiniCar and Hotel")
        return definition_list

    def get_indexes_name(self, query_definitions):
        indexes_name = [q_d.index_name for q_d in query_definitions]
        return indexes_name
