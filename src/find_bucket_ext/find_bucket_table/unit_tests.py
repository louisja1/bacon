import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

if True:
  conn = psycopg2.connect(
      dbname="toy",
      user="yl762",
      port="5600",
      host="localhost"
  )
  cur = conn.cursor()

  cur.execute(f"DROP EXTENSION IF EXISTS find_bucket_table;")
  cur.execute("CREATE EXTENSION find_bucket_table;")

  cur.execute("""
  DROP TABLE IF EXISTS rentals;

  CREATE TABLE rentals (
      id INT,
      duration INT,
      start_time TIMESTAMP
  );

  INSERT INTO rentals (id, duration, start_time) VALUES
      (1, 5, '2025-01-01 00:00:00'),
      (2, 12, '2025-01-01 01:00:00'),
      (2, NULL, '2025-01-01 00:30:01'),
      (3, 25, '2025-01-01 02:00:00'),
      (4, 31, '2025-01-01 02:00:00'),
      (999, 30, NULL),
      (18349, 29, '2025-01-01 00:59:59');         
  """)

  # Call function directly with parameters
  cur.execute("""
  SELECT *
  FROM find_bucket_table(
    'rentals',
    ARRAY['id'],
    ARRAY['duration','start_time'],   
    ARRAY[6, 4],    
    ARRAY[
      '[0,10,10,20,20,30]'::jsonb,
      '["2025-01-01 00:00:00","2025-01-01 00:30:00",
        "2025-01-01 00:31:00","2025-01-01 01:00:00"]'::jsonb
    ],
    ARRAY['int','ts'],
    ARRAY[true,false],
    'rentals.id IS NOT NULL'
  );
  """)

  rows = cur.fetchall()
  assert len(rows) == 2
  assert rows[0] == ([1], [0, 0])
  # assert rows[1] == ([2], [1, -1])
  # assert rows[2] == ([2], [0, -1])
  # assert rows[3] == ([3], [2, -1])
  # assert rows[4] == ([4], [-1, -1])
  # assert rows[5] == ([999], [-1, -1])
  assert rows[1] == ([18349], [2, 1])

  cur.execute(f"DROP EXTENSION IF EXISTS find_bucket_table;")
  cur.close()
  conn.close()
  print("Unit test 1 passed!")

if True:
  conn = psycopg2.connect(
      dbname="stats",
      user="yl762",
      port="5600",
      host="localhost"
  )
  cur = conn.cursor()

  cur.execute(f"DROP EXTENSION IF EXISTS find_bucket_table;")
  cur.execute("CREATE EXTENSION find_bucket_table;")

  q = """
  SELECT *
  FROM find_bucket_table(
      %s, 
      %s, 
      %s, 
      %s, 
      $BUCKETS_TO_REPLACE, 
      %s, 
      %s, 
      %s  
  );
  """

  buckets = [
    '[0, 1]', 
    '["2009-02-02 14:45:18.999999", "2010-07-24 06:46:49", "2010-07-24 06:46:49", "2014-09-14 02:04:27.000001"]',
  ] 

  q = q.replace("$BUCKETS_TO_REPLACE", 
                "ARRAY[" + ", ".join([f"'{b}'::jsonb" for b in buckets]) + "]")

  params = (
      'comments c', 
      ['c.userid'], 
      ['c.score','c.creationdate'], 
      [2,4], 
      ['int','ts'], 
      [False,True], 
      'c.userid >= 2 AND c.userid <= 55746'  
  )
  cur.execute(q, tuple(params))
  cur.execute(q, tuple(params))
  cur.execute(q, tuple(params))

  cur.execute(f"DROP EXTENSION IF EXISTS find_bucket_table;")
  cur.close()
  conn.close()
  print("Unit test 2 passed!")

if True:
  conn = psycopg2.connect(
      dbname="stats",
      user="yl762",
      port="5600",
      host="localhost"
  )
  cur = conn.cursor()

  cur.execute(f"DROP EXTENSION IF EXISTS find_bucket_table;")
  cur.execute("CREATE EXTENSION find_bucket_table;")

  q = """
  SELECT *
  FROM find_bucket_table(
      %s, 
      %s, 
      %s, 
      %s, 
      $BUCKETS_TO_REPLACE, 
      %s, 
      %s, 
      %s  
  );
  """

  buckets = [] 

  # q = q.replace("$BUCKETS_TO_REPLACE", 
  #               "ARRAY[" + ", ".join([f"'{b}'::jsonb" for b in buckets]) + "]")
  q = q.replace("$BUCKETS_TO_REPLACE", "'{}'")

  params = (
      'users u', 
      ['u.id'], 
      [], 
      [], 
      [], 
      [], 
      'u.id IS NOT NULL'  
  )
  # print(cur.mogrify(q, params).decode('utf-8'))
  cur.execute(q, tuple(params))

  cur.execute(f"DROP EXTENSION IF EXISTS find_bucket_table;")
  cur.close()
  conn.close()
  print("Unit test 3 passed!")
