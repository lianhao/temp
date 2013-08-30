import logging

class InvalidSortKey(Exception):
    message = "Sort key supplied was not valid."

# copy from glance/db/sqlalchemy/api.py
def paginate_query(query, model, limit, sort_keys, marker=None,
                   sort_dir=None, sort_dirs=None):
    """Returns a query with sorting / pagination criteria added.

    Pagination works by requiring a unique sort_key, specified by sort_keys.
    (If sort_keys is not unique, then we risk looping through values.)
    We use the last row in the previous page as the 'marker' for pagination.
    So we must return values that follow the passed marker in the order.
    With a single-valued sort_key, this would be easy: sort_key > X.
    With a compound-values sort_key, (k1, k2, k3) we must do this to repeat
    the lexicographical ordering:
    (k1 > X1) or (k1 == X1 && k2 > X2) or (k1 == X1 && k2 == X2 && k3 > X3)

    We also have to cope with different sort_directions.

    Typically, the id of the last row is used as the client-facing pagination
    marker, then the actual marker object must be fetched from the db and
    passed in to us as marker.

    :param query: the query object to which we should add paging/sorting
    :param model: the ORM model class
    :param limit: maximum number of items to return
    :param sort_keys: array of attributes by which results should be sorted
    :param marker: the last item of the previous page; we returns the next
                    results after this value.
    :param sort_dir: direction in which results should be sorted (asc, desc)
    :param sort_dirs: per-column array of sort_dirs, corresponding to sort_keys

    :rtype: sqlalchemy.orm.query.Query
    :return: The query with sorting/pagination added.
    """

    if 'id' not in sort_keys:
        # TODO(justinsb): If this ever gives a false-positive, check
        # the actual primary key, rather than assuming its id
        logging.info('Id not in sort_keys; is sort_keys unique?')

    assert(not (sort_dir and sort_dirs))

    # Default the sort direction to ascending
    if sort_dirs is None and sort_dir is None:
        sort_dir = 'asc'

    # Ensure a per-column sort direction
    if sort_dirs is None:
        sort_dirs = [sort_dir for _sort_key in sort_keys]

    assert(len(sort_dirs) == len(sort_keys))

    # Add sorting
    for current_sort_key, current_sort_dir in zip(sort_keys, sort_dirs):
        try:
            sort_dir_func = {
                'asc': sqlalchemy.asc,
                'desc': sqlalchemy.desc,
            }[current_sort_dir]
        except KeyError:
            raise ValueError("Unknown sort direction, "
                               "must be 'desc' or 'asc'")
        try:
            sort_key_attr = getattr(model, current_sort_key)
        except AttributeError:
            raise InvalidSortKey()
        query = query.order_by(sort_dir_func(sort_key_attr))

    # Add pagination
    if marker is not None:
        marker_values = []
        for sort_key in sort_keys:
            v = getattr(marker, sort_key)
            marker_values.append(v)

        # Build up an array of sort criteria as in the docstring
        criteria_list = []
        for i in range(0, len(sort_keys)):
            crit_attrs = []
            for j in range(0, i):
                model_attr = getattr(model, sort_keys[j])
                crit_attrs.append((model_attr == marker_values[j]))

            model_attr = getattr(model, sort_keys[i])
            if sort_dirs[i] == 'desc':
                crit_attrs.append((model_attr < marker_values[i]))
            else:
                crit_attrs.append((model_attr > marker_values[i]))

            criteria = sqlalchemy.sql.and_(*crit_attrs)
            criteria_list.append(criteria)

        f = sqlalchemy.sql.or_(*criteria_list)
        query = query.filter(f)

    if limit is not None:
        query = query.limit(limit)

    return query



import sqlalchemy
from sqlalchemy.orm import sessionmaker, relationship, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Table, ForeignKey, DateTime, \
    Index
from sqlalchemy import Float, Boolean, Text
import datetime
from sqlalchemy import func

class CeilometerBase(object):
    """Base class for Ceilometer Models."""
    __table_initialized__ = False

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def update(self, values):
        """Make the model object behave like a dict."""
        for k, v in values.iteritems():
            setattr(self, k, v)
    
Base = declarative_base(cls=CeilometerBase)

engine = sqlalchemy.create_engine('mysql://root:123456@localhost/test',
                                  echo=False)
session = sessionmaker(bind=engine)()

class Meter(Base):
    """Metering data."""

    __tablename__ = 'meter'
    __table_args__ = (
        Index('idx_meter_rid_cname', 'resource_id', 'counter_name'),
    )
    id = Column(Integer, primary_key=True)
    counter_name = Column(String(255))
    resource_id = Column(String(255), ForeignKey('resource.id'))
    counter_type = Column(String(255))
    counter_unit = Column(String(255))
    counter_volume = Column(Float(53))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)


class Resource(Base):
    __tablename__ = 'resource'
    id = Column(String(255), primary_key=True)
    user_id = Column(String(255))
    project_id = Column(String(255))
    meters = relationship("Meter", backref='resource')


def prepare_data():
    # create table
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    # create Resource
    for i in range(10):
        resource = Resource()
        resource.id = str('id%d' % i)
        resource.user_id = 'user%d' % i
        resource.project_id = 'project%d' % i
        session.add(resource)
    #create meter
    for i in range(20):
        meter = Meter()
        #meter.id = i
        meter.counter_name = 'odd' if (i/10 + i) % 2 else 'even'
        meter.resource_id = 'id%d' % (i % 10)
        meter.counter_type = 'odd' if (i/10 + i) % 2 else 'even'
        meter.counter_unit = 'odd' if (i/10 + i) % 2 else 'even'
        meter.counter_volume = i
        session.add(meter)
    session.commit()


def get_marker(model, counter_name, resource_id):
    query = session.query(model)
    query = query.filter(model.counter_name == counter_name)
    query = query.filter(model.resource_id == resource_id)
    query = query.limit(1)
    marker = query.one()
    '''print "get marker %s:%s, result %s:%s" % (counter_name,
                                              resource_id,
                                              marker.counter_name,
                                              marker.resource_id)
    '''
    return marker
    

def get_meters(counter_name=None, resource_id=None, limit=3):
    # Meter table will store large records and join with resource
        # will be very slow.
        # subquery_meter is used to reduce meter records
        # by selecting a record for each (resource_id, counter_name).
        # max() is used to choice a meter record, so the latest record
        # is selected for each (resource_id, counter_name).
        #
        subquery_meter = session.query(func.max(Meter.id).label('id')).\
            group_by(Meter.resource_id, Meter.counter_name).subquery()

        # The SQL of query_meter is essentially:
        #
        # SELECT meter.* FROM meter INNER JOIN
        #  (SELECT max(meter.id) AS id FROM meter
        #   GROUP BY meter.resource_id, meter.counter_name) AS anon_2
        # ON meter.id = anon_2.id
        #
        query_meter = session.query(Meter).\
            join(subquery_meter, Meter.id == subquery_meter.c.id)
        
        alias_meter = aliased(Meter, query_meter.subquery())
        '''query = session.query(Resource, alias_meter).join(
            alias_meter, Resource.id == alias_meter.resource_id)'''
        query = session.query(alias_meter, Resource).join(
            Resource, Resource.id == alias_meter.resource_id)
        
        if counter_name and resource_id:
            marker = get_marker(alias_meter, counter_name, resource_id)
        else:
            marker = None
        query = paginate_query(query, alias_meter, limit, ['counter_name','resource_id',], marker, sort_dir='desc')
        
        for meter, resource in query.all():
            print {
                   'name': meter.counter_name,
                   'type': meter.counter_type,
                   'unit': meter.counter_unit,
                   'resource_id': resource.id,
                   'project_id': resource.project_id,
                   'user_id': resource.user_id
                   }
        


if __name__ == '__main__':
    prepare_data()
    print "All meters"
    get_meters(limit=None)
    print "First 3 meters..."
    get_meters()
    print "Next 3 meters..."
    get_meters('odd', 'id7')
    print "Next 3 meters..."
    get_meters('odd', 'id4')
    print "Next 3 meters..."
    get_meters('odd', 'id1')
    print "Next 3 meters..."
    get_meters('even', 'id8')

            
