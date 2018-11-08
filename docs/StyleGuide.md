# TimescaleDB code style guide

Source code should follow the
[PostgreSQL coding conventions](https://www.postgresql.org/docs/current/static/source.html). This
includes
[source formatting](https://www.postgresql.org/docs/current/static/source-format.html)
with 4 column tab spacing and layout rules according to the BSD style.

## SQL and PL/pgSQL style

There is no official SQL or PL/pgSQL style guide for PostgreSQL that
we are aware of, apart from the general spacing and layout rules
above. For now, try to follow the style of the surrounding code when
making modifications. We might develop more stringent guidelines in
the future.

## Error messages

Error messages in TimescaleDB should obey the PostgreSQL
[error message style guide](https://www.postgresql.org/docs/current/static/error-style-guide.html).

## C style

While the PostgreSQL project mandates that
[C code adheres to the C89 standard](https://www.postgresql.org/docs/current/static/source-conventions.html)
with some exceptions, we've chosen to allow many C99 features for the
clarity and convenience they provide. PostgreSQL sticks with C89
mostly for compatibility with many older compilers and platforms, such
as Visual Studio on Windows. Visual Studio should support most of C99
nowadays, however. We might revisit this decision in the future if it
turns out to be a problem for important platforms.

Unfortunately, PostgreSQL does not have a consistent style for naming
of functions, variables and types. This
[mailing-list thread](https://www.postgresql.org/message-id/1221125165.5637.12.camel%40abbas-laptop)
elaborates on the situation. Instead, we've tried our best to develop
a consistent style that roughly follows the style of the PostgreSQL
source code.

### Function and variable names

For clarity and consistency, we've chosen to go with lowercase
under-score separated names for functions and variables. For instance,
this piece of code is correct:

```C
static void
my_function_name(int my_parameter)
{
    int my_variable;
    ...
}
```

while this one is wrong:

```C
static void
MyFunctionName(int myParameter)
{
    int myVariable;
    ...
}
```

### Type declarations

New composite/aggregate types should be typedef'd and use
UpperCamelCase naming. For instance, the following is correct:

```C
typedef struct MyType
{
    ...
} MyType;
```

while the following is wrong:

```C
typedef struct my_type
{
    ...
} my_type;
```

### Modular code and namespacing

When possible, code should be grouped into logical modules. Such modules
typically resemble classes in object-oriented programming (OOP)
languages and should use namespaced function and variable names that
have the module name as prefix. TimescaleDB's [Cache](../src/cache.c)
implementation is a good example of such a module where one would use

```C
void
cache_initialize(Cache *c)
{
    ...
}
```

rather than

```C
void
initialize_cache(Cache *c)
{

}
```

### Object-orientated programming style

Even though C is not an object-oriented programming language, it is
fairly straight-forward to write C code with an OOP flavor. While we
do not mandate that C code has an OOP flavor, we recommend it when it
makes sense (e.g., to achieve modularity and code reuse).

For example, TimescaleDB's [cache.c](../src/cache.c) module can be
seen as a _base class_ with multiple derived _subclasses_, such as
[hypertable_cache.c](../src/hypertable_cache.c) and
[chunk_cache.c](../src/chunk_cache.c). Here's another example of
subclassing using shapes:

```C
typedef struct Shape
{
    int color;
    void (*draw)(Shape *, Canvas *);
} Shape;

void
shape_draw(Shape *shape)
{
    /* open canvas for drawing */
    Canvas *c = canvas_open();

    /* other common shape code */
    ...

    shape->draw(shape, c);

    canvas_close(c);
}

typedef struct Circle
{
    Shape shape;
    float diameter;
} Circle;

Circle blue_circle = {
    .shape = {
        .color = BLUE,
        .draw = circle_draw,
    },
    .diameter = 10.1,
};

void
circle_draw(Shape *shape, Canvas *canvas)
{
    Circle *circle = (Circle *) shape;

    /* draw circle */
    ...
}

```

There are a couple of noteworthy take-aways from this
example.

* Non-static "member" methods should take a pointer to the
object as first argument and be namespaced as described above.
* Derived modules can expand the original type using struct embedding,
in which case the "superclass" should be the first member of the
"subclass", so that one can easily upcast a `Circle` to a `Shape` or,
vice-versa, downcast as in `circle_draw()` above.
* C++-style virtual functions can be implemented with functions
pointers. Good use cases for such functions are module-specific
initialization and cleanup, or function overriding in a subclass.

### Other noteworthy recommendations

* Prefer static allocation and/or stack-allocated variables over
  heap/dynamic allocation when possible. Fortunately, PostgreSQL has a
  nice `MemoryContext` implementation that helps with heap allocation
  when needed.
* Try to minimize the number of included headers in source files,
  especially when header files include other headers. Avoid circular
  header dependencies by predeclaring types (or use struct pointers).


For a general guide to writing C functions in PostgreSQL, you can
review the section on
[C-language functions](https://www.postgresql.org/docs/current/static/xfunc-c.html)
in the PostgreSQL documentation.

## Tools and editors

We require running C code through clang-format before submitting a PR.
This will ensure your code is properly formatted according to our style
(which is similar to the PostgreSQL style but implement in clang-format).
You can run clang-format on all of the TimescaleDB code using `make format`
if you have clang-format (version >= 7) or docker installed.


The following
[Wiki post](https://wiki.postgresql.org/wiki/Developer_FAQ#What.27s_the_formatting_style_used_in_PostgreSQL_source_code.3F)
contains links to style configuration files for various editors.
