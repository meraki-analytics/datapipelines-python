Data Pipeline
=============

A data pipeline collects, stores, and processes data. This package provides a framework for facilitating this process.

Data is collected from ``DataSources``, stored in ``DataSinks``, and processed using ``Transformers``.

``DataSources`` are entities that *provide* data to the pipeline. Examples include databases, in-memory caches, and REST APIs.

``DataSinks`` are entities that *store* data provided by ``DataSources``. Examples include databases and in-memory caches. Nearly all data sinks will also be data sources because storing data is usually unhelpful if you cannot get that data out. We refer to an entity that is both a data source and data sink as a *data store*.

``Transformers`` are entities that *transform* or process data from one data type to another. For example, a transformer may transform a Word document to a PDF.

The ``DataPipeline`` consists of a list of ``DataStores`` and ``DataSinks`` that communicate via ``Transformers``.

The data sources and sinks are ordered in the data pipeline, and their order determines the order in which data is requested. Generally speaking, slower data stores/sinks should go towards the end of the pipeline.

Not every data type needs to be supported by every data sink or data store. If a data sink/store does not support a requested type of data, that data sink/source is simply skipped in the pipeline.

Example
-------

For example, if your data pipeline consists of an in-memory cache, a database, and a REST API service (in that order), when you perform a query, the ``DataPipeline`` will first look in the in-memory cache, then in the database, then in the REST API. If the data is found in the cache, it will be returned and the database and REST API will not be queried. Similarly, if the data is found in the database, the REST API will not be queried.

After data is found in a data source, the data propagates back down the data pipeline from whence it came. Any data sink encountered along the way will store that data. So, continuing the above example, if your query was provided by the REST API, the returned data would be stored in your database, then stored in the cache. A data sink will only store data that it supports.

Each data sink can define expiration periods for each type of data it supports, but this is up to the specific data sink to implement.

Usage
-----

The below code below is simplified code to illustrate an example of converting a Word document, which is requested from an SQL database, to a PDF document.

Note that no PDF documents are stored in the database, but the data pipeline can still return one if it is requested.

.. code-block:: python

    # The four classes below implement a simple DataPipeline. The code would need to be filled in by the user.
    
    class WordDoc:
        ...
    
    class PDF:
        ...
     
    class SQLDatabase(DataSource, DataSink):
        @get.register(WordDoc)  # Tells the DataPipeline that this SQL database can provide a WordDoc
        def get_word_doc(query: Dict[str, Any]) -> WordDoc:
            """Returns a WordDoc from an SQL database based on the `filename` in the query."""
            
        @put.register(WordDoc)  # Tell the DataPipeline that this SQL database can store a WordDoc
        def put_word_doc(doc: WordDoc, query: Dict[str, Any]):
            """Stores the document in the SQL database using the query as an identifier."""
            
    class DocumentTransformer(Transformer):
        @transform.register(WordDoc, PDF)  # Tells the DataPipeline that we know how to convert a WordDoc to a PDF
        def Word_to_PDF(doc: WordDoc) -> PDF:
            """Converts a WordDoc to a PDF and returns the PDF."""
        
        
    # The line of code below can now be used to request a PDF.
    # The WordDoc with the filename `find_me` will be pulled from the SQL database then converted to a PDF and returned to the user.
    my_pdf = pipeline.get(PDF, query={"filename": "find_me"})

    # Note also that because we implemented a `put(WordDoc)` method in the SQLDatabase that it will also store WordDocs that pass through the SQL database via the pipeline but are not already in the database.
