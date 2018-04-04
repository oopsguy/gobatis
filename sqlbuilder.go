package sql

// A SQL statement builder which refrence apache MyBatis SQL class

import (
	"bytes"
)

const (
	stmConstDelete = iota + 1
	stmConstInsert
	stmConstSelect
	stmConstUpdate

	logicAND = ") \nAND ("
	logicOR = ") \nOR ("
)

type appendable interface {
	WriteString(s string) (int, error)
	String() string
}

type safeAppendable struct {
	appender appendable
	empty bool
}

func newSafeAppendable(a appendable) *safeAppendable {
	return &safeAppendable{
		appender: a,
		empty: true,
	}
}

func (this *safeAppendable) append(s string) *safeAppendable {
	if this.empty && len(s) > 0 {
		this.empty = false
	}
	_, err := this.appender.WriteString(s)
	if err != nil {
		panic(err)
	}
	return this
}

func (this *safeAppendable) String() string {
	return this.appender.String()
}

type statement struct {
	stmConstType int
	sets []string
	selects []string
	tables []string
	join []string
	innerJoin []string
	outerJoin []string
	leftOuterJoin []string
	rightOuterJoin []string
	where *[]string
	having []string
	groupBy []string
	orderBy []string
	lastList *[]string
	columns []string
	values []string
	distinct bool
}

func newStatement() *statement {
	return &statement{
		sets: []string{},
		selects: []string{},
		tables: []string{},
		join: []string{},
		innerJoin: []string{},
		outerJoin: []string{},
		leftOuterJoin: []string{},
		rightOuterJoin: []string{},
		where: &[]string{},
		having: []string{},
		groupBy: []string{},
		orderBy: []string{},
		lastList: &[]string{},
		columns: []string{},
		values: []string{},
	}
}

func (this *statement) sqlClause(builder *safeAppendable, keyword string, parts []string, open string, close string, conjunction string) {
	if parts == nil || len(parts) == 0 {
		return
	}

	if !builder.empty {
		builder.append("\n")
	}
	builder.append(keyword)
	builder.append(" ")
	builder.append(open)
	
	last := "________"
	i := 0
	// TODO ++i NO i++
	for n:=len(parts); i<n; i++ {
		part := parts[i]
		if i > 0 && part != logicAND && part != logicOR && last != logicAND && last != logicOR {
			builder.append(conjunction)
		}
		builder.append(part)
		last = part
	}

	builder.append(close)
}

func (this *statement) selectsqlBuilder(builder *safeAppendable) string {
	if this.distinct {
		this.sqlClause(builder, "SELECT DISTINCT", this.selects, "", "", ", ")
	} else {
		this.sqlClause(builder, "SELECT", this.selects, "", "", ", ")
	}

	this.sqlClause(builder, "FROM", this.tables, "", "", ", ")
	this.joins(builder)
	this.sqlClause(builder, "WHERE", *this.where, "(", ")", " AND ")
	this.sqlClause(builder, "GROUP BY", this.groupBy, "", "", ", ")
	this.sqlClause(builder, "HAVING", this.having, "(", ")", " AND ")
	this.sqlClause(builder, "ORDER BY", this.orderBy, "", "", ", ")
	return builder.String()
}

func (this *statement) joins(builder *safeAppendable) {
	this.sqlClause(builder, "JOIN", this.join, "", "", "\nJOIN ")
	this.sqlClause(builder, "INNER JOIN", this.innerJoin, "", "", "\nINNER JOIN ")
	this.sqlClause(builder, "OUTER JOIN", this.outerJoin, "", "", "\nOUTER JOIN ")
	this.sqlClause(builder, "LEFT OUTER JOIN", this.leftOuterJoin, "", "", "\nLEFT OUTER JOIN ")
	this.sqlClause(builder, "RIGHT OUTER JOIN", this.rightOuterJoin, "", "", "\nRIGHT OUTER JOIN ")
}

func (this *statement) insertsqlBuilder(builder *safeAppendable) string {
	this.sqlClause(builder, "INSERT INTO", this.tables, "", "", "")
	this.sqlClause(builder, "", this.columns, "(", ")", ", ")
	this.sqlClause(builder, "VALUES", this.values, "(", ")", ", ")
	return builder.String()
}

func (this *statement) deletesqlBuilder(builder *safeAppendable) string {
	this.sqlClause(builder, "DELETE FROM", this.tables, "", "", "")
	this.sqlClause(builder, "WHERE", *this.where, "(", ")", " AND ")
	return builder.String()
}

func (this *statement) updatesqlBuilder(builder *safeAppendable) string {
	this.sqlClause(builder, "UPDATE", this.tables, "", "", "")
	this.joins(builder)
	this.sqlClause(builder, "SET", this.sets, "", "", ", ")
	this.sqlClause(builder, "WHERE", *this.where, "(", ")", " AND ")
	return builder.String()
}

func (this *statement) sql(a appendable) string {
	builder := newSafeAppendable(a)
	if this.stmConstType == 0 {
		return ""
	}

	answer := ""
	switch this.stmConstType {
		case stmConstDelete:
			answer = this.deletesqlBuilder(builder);
			break;
		case stmConstInsert:
			answer = this.insertsqlBuilder(builder);
			break;
		case stmConstSelect:
			answer = this.selectsqlBuilder(builder);
			break;
		case stmConstUpdate:
			answer = this.updatesqlBuilder(builder);
			break;
	}

	return answer
}

// SQL Builder facade, expose apis which used to build sql statement
type sqlBuilder struct {
	sqlStm *statement
}

func NewSqlBuilder() *sqlBuilder {
	return &sqlBuilder{
		sqlStm: newStatement(),
	}
}

func (this *sqlBuilder) UPDATE(table string) *sqlBuilder {
	this.sqlStm.stmConstType = stmConstUpdate
	this.sqlStm.tables = append(this.sqlStm.tables, table)
	return this
}

func (this *sqlBuilder) SET(sets ...string) *sqlBuilder {
	this.sqlStm.sets = append(this.sqlStm.sets, sets...)
	return this
}

func (this *sqlBuilder) INSERT_INTO(table string) *sqlBuilder {
	this.sqlStm.stmConstType = stmConstInsert
	this.sqlStm.tables = append(this.sqlStm.tables, table)
	return this
}

func (this *sqlBuilder) VALUES(columns string, values string) *sqlBuilder {
	this.sqlStm.columns = append(this.sqlStm.columns, columns)
	this.sqlStm.values = append(this.sqlStm.values, values)
	return this
}

func (this *sqlBuilder) INTO_COLUMNS(columns ...string) *sqlBuilder {
	this.sqlStm.columns = append(this.sqlStm.columns, columns...)
	return this
}

func (this *sqlBuilder) INTO_VALUES(values ...string) *sqlBuilder {
	this.sqlStm.values = append(this.sqlStm.values, values...)
	return this
}

func (this *sqlBuilder) SELECT(columns ...string) *sqlBuilder {
	this.sqlStm.stmConstType = stmConstSelect
	this.sqlStm.selects = append(this.sqlStm.selects, columns...)
	return this
}

func (this *sqlBuilder) SELECT_DISTINCT(columns ...string) *sqlBuilder {
	this.sqlStm.distinct = true
	this.SELECT(columns...)
	return this
}

func (this *sqlBuilder) DELETE_FROM(table string) *sqlBuilder {
	this.sqlStm.stmConstType = stmConstDelete
	this.sqlStm.tables = append(this.sqlStm.tables, table)
	return this
}

func (this *sqlBuilder) FROM(tables ...string) *sqlBuilder {
	this.sqlStm.tables = append(this.sqlStm.tables, tables...)
	return this
}

func (this *sqlBuilder) JOIN(joins ...string) *sqlBuilder {
	this.sqlStm.join = append(this.sqlStm.join, joins...)
	return this
}

func (this *sqlBuilder) INNER_JOIN(joins ...string) *sqlBuilder {
	this.sqlStm.innerJoin = append(this.sqlStm.innerJoin, joins...)
	return this
}

func (this *sqlBuilder) LEFT_OUTER_JOIN(joins ...string) *sqlBuilder {
	this.sqlStm.leftOuterJoin = append(this.sqlStm.leftOuterJoin, joins...)
	return this
}

func (this *sqlBuilder) RIGHT_OUTER_JOIN(joins ...string) *sqlBuilder {
	this.sqlStm.rightOuterJoin = append(this.sqlStm.rightOuterJoin, joins...)
	return this
}

func (this *sqlBuilder) OUTER_JOIN(joins ...string) *sqlBuilder {
	this.sqlStm.outerJoin = append(this.sqlStm.outerJoin, joins...)
	return this
}

func (this *sqlBuilder) WHERE(conditions ...string) *sqlBuilder {
	sliceRefAppend(this.sqlStm.where, conditions...)
	this.sqlStm.lastList = this.sqlStm.where;
	return this
}

func (this *sqlBuilder) OR() *sqlBuilder {
	sliceRefAppend(this.sqlStm.lastList, logicOR)
	return this
}

func (this *sqlBuilder) AND() *sqlBuilder {
	sliceRefAppend(this.sqlStm.lastList, logicAND)
	return this
}

func (this *sqlBuilder) GROUP_BY(columns ...string) *sqlBuilder {
	this.sqlStm.groupBy = append(this.sqlStm.groupBy, columns...)
	return this
}

func (this *sqlBuilder) HAVING(conditions ...string) *sqlBuilder {
	this.sqlStm.having = append(this.sqlStm.having, conditions...)
	return this
}

func (this *sqlBuilder) ORDER_BY(columns ...string) *sqlBuilder {
	this.sqlStm.orderBy = append(this.sqlStm.orderBy, columns...)
	return this
}

func (this *sqlBuilder) String() string {
	return this.sqlStm.sql(&bytes.Buffer{})
}

func (this *sqlBuilder) Clear() *sqlBuilder {
	this.sqlStm = newStatement()
	return this
}

func sliceRefAppend(slice *[]string, val ...string) {
	*slice = append(*slice, val...)
}
