Yape - Yet another pipeline executor
####################################

Yape is a general-purpose pipeline/workflow executor written in Python. It
provides ways to construct and run an execution graph.

Yape's features:

- **Ergonomics of Python**: The execution graph is created via Python code,
  providing great flexibility on how you create and configure your execution
  nodes. The API tries its best to provide an syntax-sugary way of creating
  and using nodes.

- **Graph Execution**: Graph execution can be done via CLI or a Python API. You
  can execute the whole graph or selected a set of target nodes for execution.

- **Node dependencies**: A node can use as dependencies a mixture of regular
  python values, other nodes and file system paths. When nodes are used as
  dependencies, Yape will know how to execute them in the correct order and to
  provide the results to the dependent nodes.

- **Caching**: By default, if Yape sees that an execution node did not change
  from its last run, it does not re-execute it and just return the cached
  result. This behavior can be turned off if necessary.

- **Minimal execution sub-graph**: Yape is capable of generating and exporting a
  minimal sub-graph with only the nodes/data necessary to fulfill a selected set
  of target nodes. This is specially useful for machine learning pipelines into
  production, where one is only interested in the execution path for the "test
  phase".


Install
=======

Yape can be installed via pip:

.. code::

    pip install yape


Quickstart
==========

Creating the execution graph
----------------------------

By default, the ``yape`` command looks for and load the file ``yp.py``, which is
the entry point for creating the execution graph. In general, that file would be
at the root directory of your project. In the Python code examples provided
here, unless explicitly noted, the content is expected to be in the ``yp.py``
file.

The most common way of creating a node is by wrapping a function responsible for
the execution of such a node. There are multiple ways you can create functional
nodes:

1. Using the ``yp.node`` decorator:

   .. code:: python

       import yape as yp

       @yp.node
       def hello(who="world"):
           print(f"Hello, {who}!")

   When using ``@yp.node``, ``hello`` is transformed into a ``yape.Node``
   object, which will execute the original function.

2. Using the ``yp.fn`` decorator:

   .. code:: python

      import yape as yp

      @yp.fn
      def hello(who):
          print(f"Hello, {who}!")

      def nodegen():
          hello("world")

   In this example, ``hello`` is transformed into a special kind of function
   called a *node generator* and we use it inside ``nodegen()``.

   When a node generator is called, it creates a new node object that will call
   the wrapped function with the same arguments passed to the generator.

   The function ``nodegen()`` is a special function that Yape looks for when
   loading the ``yp.py`` file and is understood as the function responsible for
   creating the node objects for ``yp.py`` [#nodegen_in_style_1]_.

3. Using ``yp.fn`` decorator directly for node creation:

   .. code:: python

      import yape as yp

      def hello(who):
          print(f"Hello, {who}!")

      def nodegen():
          yp.fn(hello)("world")

   This has the same effect achieved with example (2), but with the difference
   that ``hello`` is left untouched, which could be useful if you want to keep
   your functions unmodified for other uses. This is also useful when using
   functions provided by other libraries.

.. [#nodegen_in_style_1] Note that in example (1) we did not create a
   ``nodegen()`` function as the node is already created by ``@yp.node``, but
   you could have a ``nodegen()`` there as well (in case you use a mixture of
   node creation styles).


Running
-------

You can use the command ``yape`` to run your execution graph. Using the example
from the above:

.. code::

   $ yape
   Hello, world!


Ignoring the cache
''''''''''''''''''

If you try running it again, you will see that there will be no output:

.. code::

   $ yape


That's because the node hasn't changed, so Yape knows it does not have to
execute it. If we change the node definition or arguments, then Yape will detect
the change. For example, let's change the argument for our node:

.. code:: python

   import yape as yp

   def hello(who):
       print(f"Hello, {who}!")

   def nodegen():
       yp.fn(hello)("my friend")


And then run ``yape``:

.. code::

   $ yape
   Hello, my friend!


The command ``yape`` without positional arguments is actually a shortcut for
``yape run``, which is the sub-command responsible for running the execution
graph. If you want to force the execution of nodes and ignore the cache, you can
use the ``-f`` option (short for ``--force``):

.. code::

   $ yape run -f
   Hello, my friend!


Selecting target nodes
''''''''''''''''''''''

The ``yape run`` sub-command also allows us to select which nodes we want to
execute. Let's increment our example by defining extra nodes:

.. code:: python

   import yape as yp

   def hello(who):
       print(f"Hello, {who}!")

   def hi(who):
       print(f"Hi, {who}!")

   def nodegen():
       yp.fn(hello)("my friend")
       yp.fn(hello, name="hello_world")("world")
       yp.fn(hi)("John Doe")

We created two extra nodes. By default, a functional node will be named after
the name of the wrapped function. Since the first node already will be named
"hello", we explicitly define a different name ("hello_world") for the second
one.

We can select nodes to be run by passing their names (or paths when they belong
to sub-graphs) as positional arguments:

.. code::

   $ yape run hello_world
   Hello, world!

.. code::

   $ yape run -f hello hi # Using -f because hello is cached
   Hi, John Doe!
   Hello, my friend!
