unit class SQL::Cantrip:ver<0.0.1>:auth<cpan:avuserow>;

has $.db where *.can('quote');

my class Statement {
    has Str $.sql is readonly;
    has @.bind is readonly;

    method things {
        return [$.sql, @.bind];
    }
};

my class Fragment is Statement {}

my class Group {
    has $.type;
    has @.items;
}

method compare($column, $op, $value) {
    given $op.uc {
        when any(qww/< <= = != >= > IS LIKE ILIKE "NOT LIKE" "NOT ILIKE" "IS NOT"/) {
            if $value.defined {
                Fragment.new(:sql("$!db.quote($column) $op.uc() ?"), :bind[$value]);
            } else {
                Fragment.new(:sql("$!db.quote($column) $op.uc() NULL"), :bind[]);
            }
        }
        when 'IN' | 'NOT IN' {
            my $placeholders = join ', ', ('?' xx $value.elems);
            Fragment.new(:sql("$!db.quote($column) $op.uc() ($placeholders)"), :bind(|$value));
        }
        when 'BETWEEN' | 'NOT BETWEEN' {
            if $value.elems != 2 {
                die "Invalid use of operator $op: requires exactly two values, not {$value.elems}";
            }
            Fragment.new(:sql("$!db.quote($column) $op.uc() ? AND ?"), :bind(|$value));
        }
        default {
            die "Unknown operator $op";
        }
    }
}

multi method group(@items, :$or!) {
    Group.new(:type('OR'), :@items);
}

multi method group(@items, :$and!) {
    Group.new(:type('AND'), :@items);
}

method select(Str $table, @where, :$cols) {
    # TODO: limit/offset/etc
    # TODO: ordering
    my $colspec = $cols ?? join ', ', ($!db.quote($_) for |$cols) !! '*';
    my $sql = "SELECT $colspec FROM $!db.quote($table)";

    my ($where, $where-bind) = self!where-clause(@where);
    $sql ~= $where;
    my @bind = |$where-bind;

    return Statement.new(:$sql, :@bind);
}

method insert(Str $table, %set) {
    my $sql = "INSERT INTO $!db.quote($table)";
    my (@keys, @bind);

    for %set.sort -> $p {
        push @keys, $!db.quote($p.key);
        push @bind, $p.value;
    }

    $sql ~= " ({join ', ', @keys})";
    $sql ~= " values ({join ', ', '?' xx @keys.elems})";
    return Statement.new(:$sql, :@bind);
}

method update(Str $table, %set, @where) {
    my $sql = "UPDATE $!db.quote($table) SET ";

    my (@keys, @bind);
    for %set.sort -> $p {
        if $p.value.defined {
            push @keys, "{$!db.quote($p.key)} = ?";
            push @bind, $p.value;
        } else {
            push @keys, "{$!db.quote($p.key)} = NULL";
        }
    }
    $sql ~= @keys.join(', ');

    my ($where, $where-bind) = self.where(@where);
    $sql ~= ' ' ~ $where;
    push @bind, |$where-bind;

    return Statement.new(:$sql, :@bind);
}

method delete(Str $table, @where) {
    my $sql = "DELETE FROM $!db.quote($table)";
    my ($where, $where-bind) = self.where(@where);
    $sql ~= ' ' ~ $where;
    return Statement.new(:$sql, :bind(|$where-bind));
}

method where(@where) {
    return Statement.new(:sql(""), :bind[]) unless @where;
    my ($sql, @bind) = self!inner-where(@where);
    return Statement.new(:sql("WHERE $sql"), :@bind);
}

method !where-clause(@where) {
    return "", [] unless @where;
    my ($sql, @bind) = self!inner-where(@where);
    return " WHERE $sql", @bind;
}

method !inner-where(@where, :$logic-operator = 'AND') {
    my (@parts, @bind);

    for @where -> $item {
        given $item {
            when Group {
                my ($inner-sql, @inner-bind) = self!inner-where($item.items, :logic-operator($item.type));
                push @parts, "($inner-sql)";
                append @bind, @inner-bind;
            }
            when Fragment {
                push @parts, $item.sql;
                append @bind, $item.bind;
            }
            when Pair {
                if $item.value.defined {
                    push @parts, "$!db.quote($item.key) = ?";
                    push @bind, $item.value;
                } else {
                    push @parts, "$!db.quote($item.key) IS NULL";
                }
            }
            default {
                die "Don't know how to handle a $_.WHAT()";
            }
        }
    }

    return join(" $logic-operator ", @parts), |@bind;
}

=begin pod

=head1 NAME

SQL::Cantrip - generate simple SQL statements

=head1 SYNOPSIS

=begin code :lang<raku>

use SQL::Cantrip;

my $sql = SQL::Cantrip.new(:$db);

# Insert values
my $stmt = $sql.insert("users", {:$name, :$email});
$db.execute($stmt.sql, $stmt.bind);

# Select values
my $stmt = $sql.select("users", [:name<CoolDude>], :cols<name email>);
my @users = $db.execute($stmt.sql, $stmt.bind).allrows;

# Update values
my $stmt = $sql.update("users", {:email($new-email)}, [:name<CoolDude>]);
$db.execute($stmt.sql, $stmt.bind);

# Delete values
my $stmt = $sql.delete("users", [:name<CoolDude>]);
$db.execute($stmt.sql, $stmt.bind);

=end code

=head1 DESCRIPTION

SQL::Cantrip is a module for generating SQL statements from lists of Raku values, with helper methods to indicate other operators and parenthesized groups.

This module aims to make it easy and safe to do simple operations with varying columns, such as for a search form allowing users to specify multiple columns to search.

SQL::Cantrip is not an ORM. This may work better for simple applications or when you want to avoid having an object per row.

SQL::Cantrip does not try to support many parts of SQL. Handwritten SQL can be more reliable and clearer for more complex queries. SQL::Cantrip does provide a C<where> method, which generates a C<WHERE> clause that can be combined with handwritten SQL.

=head1 ATTRIBUTES

Attributes for SQL::Cantrip.

=head2 db

The database handle. Typically this is a handle from L<DBIish>. This object must have a C<quote> method that is suitable for quoting column names safely (or escaping the names).

=head1 SQL GENERATION METHODS

=head2 select(Str $table, @where, :$cols)

Generate a C<SELECT> statement for the provided C<$table>. The C<@where> parameter is passed to the C<where> method, see below for usage. .

C<$cols> is an optional parameter that is either a Str or a list of Strs. If provided, these are quoted and used as columns to select. Otherwise, C<*> is used.

This method returns a C<Statement> object, documented below.

=head2 insert(Str $table, %set)

Generate an C<INSERT> statement for the provided C<$table>. The hash C<%set> provides the columns to update with their corresponding values as key/value pairs.

This method returns a C<Statement> object, documented below.

=head2 update(Str $table, %set, @where)

Generate an C<UPDATE> statement for the provided C<$table>. Like C<insert>, it takes a hash C<%set> of columns and their new values. Like C<select>, the C<@where> value is passed to the C<where> method, documented below.

The C<@where> clause is required. If you intend to update the entire table, pass an empty list.

This method returns a C<Statement> object, documented below.

=head2 delete(Str $table, @where)

Generate a C<DELETE> statement for the provided C<$table>. Like C<select>, the C<@where> value is passed to the C<where> method, documented below.

The C<@where> clause is required. If you intend to delete all rows in the table, pass an empty list.

This method returns a C<Statement> object, documented below.

=head2 where(@where)

This method generates a C<WHERE> clause. It takes a list of values and joins them together with C<AND>. To use C<OR> instead, use a C<group>.

The where clause syntax is a list of values. C<Pair>s are used for comparing for equality (or checking null). Other comparisons (and parenthesized groups) are generated by helper methods.

To provide more safety, these helper methods all return objects, which protects against a potential trap when accepting JSON values directly (see L<<http://blog.kazuhooku.com/2014/07/the-json-sql-injection-vulnerability.html>> for details). As a benefit, these helper methods also allow for better error reporting.

Values are processed as following:

=head3 Pair

Pairs are simple equality comparisons for the given column (the key) and the value (the value). This generates SQL of the form C<"column" = ?> (with C<column> quoted appropriately, and with C<value> in the bind parameters).

If the Pair's value is not defined, then the SQL is instead C<"column" IS NULL>.

=head3 compare($column, $operator, $value) method call

Use the C<compare($column, $operator, $value)> method to compare using the provided operator. In general, this SQL is in the form C<"column" $operator ?> (with C<column> quoted appropriately, and with C<value> in the bind parameters).

See C<compare> documentation below for more on the operators.

=head3 group(@where, :and | :or)

Use the C<group()> method to make a parenthesized group of items, joined by either C<AND> or C<OR> depending whether C<:and> or C<:or> is passed. The C<@where> value here is passed recursively into the C<where()> method.

The C<where> method returns a C<Statement> object, documented below.

=head1 METHODS FOR WHERE CLAUSES

=head2 group(@where, :or)

Generate a parenthesized group in a C<where> clause, joined by C<OR> rather than the default of C<AND>.

=head2 group(@where, :and)

Generate a parenthesized group in a C<where> clause, joined by C<AND>.

=head2 compare($column, $operator, $value)

Generate a comparison in a C<where> clause using the specified operator. The following operators are permitted:

Standard comparison operators: C<< < >>, C<< <= >>, C<=>, C<!=>, C<< >= >>, C<< > >>, C<IS>, C<LIKE>, C<NOT LIKE>, C<IS NOT>. If C<$value> is defined, generates C<"$column" $operator ?> (and puts C<$value> into the bind list), otherwise generates C<"$column" $operator NULL>.

The PostgreSQL operators C<ILIKE> and C<NOT ILIKE>, which are case-insensitive versions of C<LIKE>, otherwise same as above

The operators C<IN> and C<NOT IN>: treats C<$value> as a List and generates a bind query for each value. Does not handle C<NULL> specially. Generates C<"$column" IN (?, ?, ?)> (for a three element C<$value>, for instance).

The operators C<BETWEEN> and C<NOT BETWEEN>: treats C<$value> as a List of exactly two elements and generates a bind query for each value. Does not handle C<NULL> specially. Generates C<"$column" BETWEEN ? AND ?>.

=head1 OTHER CLASSES

=head2 Statement

All methods return an instance of this class. It has two public attributes:

=item C<sql> - the generated SQL

=item C<bind> - list of bound parameters

This is meant to be used directly with a database handle (such as from C<DBIish>):

    my $stmt = $sql.insert("users", %user-data);
    $db.execute($stmt.sql, $stmt.bind);


=head1 AUTHOR

Adrian Kreher <avuserow@gmail.com>

=head1 COPYRIGHT AND LICENSE

Copyright 2021 Adrian Kreher

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
