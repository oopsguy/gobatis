package gobatis

// An SQL builder inspired by MyBatis SQL class

import (
	"bytes"
)

const (
	stmConstDelete = iota + 1
	stmConstInsert
	stmConstSelect
	stmConstUpdate

	logicAND = ") \nAND ("
	logicOR  = ") \nOR ("
)

type appendable interface {
	WriteString(s string) (int, error)
	String() string
}

type safeAppendable struct {
	appender appendable
	empty    bool
}

func newSafeAppendable(a appendable) *safeAppendable {
	return &safeAppendable{
		appender: a,
		empty:    true,
	}
}

func (s *safeAppendable) append(str string) *safeAppendable {
	if s.empty && len(str) > 0 {
		s.empty = false
	}
	_, err := s.appender.WriteString(str)
	if err != nil {
		panic(err)
	}
	return s
}

func (s *safeAppendable) String() string {
	return s.appender.String()
}

type statement struct {
	stmConstType   int
	sets           []string
	selects        []string
	tables         []string
	join           []string
	innerJoin      []string
	outerJoin      []string
	leftOuterJoin  []string
	rightOuterJoin []string
	where          *[]string
	having         []string
	groupBy        []string
	orderBy        []string
	lastList       *[]string
	columns        []string
	values         []string
	distinct       bool
}

func newStatement() *statement {
	return &statement{
		sets:           []string{},
		selects:        []string{},
		tables:         []string{},
		join:           []string{},
		innerJoin:      []string{},
		outerJoin:      []string{},
		leftOuterJoin:  []string{},
		rightOuterJoin: []string{},
		where:          &[]string{},
		having:         []string{},
		groupBy:        []string{},
		orderBy:        []string{},
		lastList:       &[]string{},
		columns:        []string{},
		values:         []string{},
	}
}

func (s *statement) sqlClause(builder *safeAppendable, keyword string, parts []string, open string, close string, conjunction string) {
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
	for n := len(parts); i < n; i++ {
		part := parts[i]
		if i > 0 && part != logicAND && part != logicOR && last != logicAND && last != logicOR {
			builder.append(conjunction)
		}
		builder.append(part)
		last = part
	}

	builder.append(close)
}

func (s *statement) selectSqlBuilder(builder *safeAppendable) string {
	if s.distinct {
		s.sqlClause(builder, "SELECT DISTINCT", s.selects, "", "", ", ")
	} else {
		s.sqlClause(builder, "SELECT", s.selects, "", "", ", ")
	}

	s.sqlClause(builder, "FROM", s.tables, "", "", ", ")
	s.joins(builder)
	s.sqlClause(builder, "WHERE", *s.where, "(", ")", " AND ")
	s.sqlClause(builder, "GROUP BY", s.groupBy, "", "", ", ")
	s.sqlClause(builder, "HAVING", s.having, "(", ")", " AND ")
	s.sqlClause(builder, "ORDER BY", s.orderBy, "", "", ", ")
	return builder.String()
}

func (s *statement) joins(builder *safeAppendable) {
	s.sqlClause(builder, "JOIN", s.join, "", "", "\nJOIN ")
	s.sqlClause(builder, "INNER JOIN", s.innerJoin, "", "", "\nINNER JOIN ")
	s.sqlClause(builder, "OUTER JOIN", s.outerJoin, "", "", "\nOUTER JOIN ")
	s.sqlClause(builder, "LEFT OUTER JOIN", s.leftOuterJoin, "", "", "\nLEFT OUTER JOIN ")
	s.sqlClause(builder, "RIGHT OUTER JOIN", s.rightOuterJoin, "", "", "\nRIGHT OUTER JOIN ")
}

func (s *statement) insertSqlBuilder(builder *safeAppendable) string {
	s.sqlClause(builder, "INSERT INTO", s.tables, "", "", "")
	s.sqlClause(builder, "", s.columns, "(", ")", ", ")
	s.sqlClause(builder, "VALUES", s.values, "(", ")", ", ")
	return builder.String()
}

func (s *statement) deleteSqlBuilder(builder *safeAppendable) string {
	s.sqlClause(builder, "DELETE FROM", s.tables, "", "", "")
	s.sqlClause(builder, "WHERE", *s.where, "(", ")", " AND ")
	return builder.String()
}

func (s *statement) updateSqlBuilder(builder *safeAppendable) string {
	s.sqlClause(builder, "UPDATE", s.tables, "", "", "")
	s.joins(builder)
	s.sqlClause(builder, "SET", s.sets, "", "", ", ")
	s.sqlClause(builder, "WHERE", *s.where, "(", ")", " AND ")
	return builder.String()
}

func (s *statement) sql(a appendable) string {
	builder := newSafeAppendable(a)
	if s.stmConstType == 0 {
		return ""
	}

	answer := ""
	switch s.stmConstType {
	case stmConstDelete:
		answer = s.deleteSqlBuilder(builder)
	case stmConstInsert:
		answer = s.insertSqlBuilder(builder)
	case stmConstSelect:
		answer = s.selectSqlBuilder(builder)
	case stmConstUpdate:
		answer = s.updateSqlBuilder(builder)
	}

	return answer
}

type sqlBuilder struct {
	sqlStm *statement
}

func NewSqlBuilder() *sqlBuilder {
	return &sqlBuilder{
		sqlStm: newStatement(),
	}
}

func (s *sqlBuilder) Update(table string) *sqlBuilder {
	s.sqlStm.stmConstType = stmConstUpdate
	s.sqlStm.tables = append(s.sqlStm.tables, table)
	return s
}

func (s *sqlBuilder) Set(sets ...string) *sqlBuilder {
	s.sqlStm.sets = append(s.sqlStm.sets, sets...)
	return s
}

func (s *sqlBuilder) InserInto(table string) *sqlBuilder {
	s.sqlStm.stmConstType = stmConstInsert
	s.sqlStm.tables = append(s.sqlStm.tables, table)
	return s
}

func (s *sqlBuilder) Values(columns string, values string) *sqlBuilder {
	s.sqlStm.columns = append(s.sqlStm.columns, columns)
	s.sqlStm.values = append(s.sqlStm.values, values)
	return s
}

func (s *sqlBuilder) IntoColumns(columns ...string) *sqlBuilder {
	s.sqlStm.columns = append(s.sqlStm.columns, columns...)
	return s
}

func (s *sqlBuilder) IntoValues(values ...string) *sqlBuilder {
	s.sqlStm.values = append(s.sqlStm.values, values...)
	return s
}

func (s *sqlBuilder) Select(columns ...string) *sqlBuilder {
	s.sqlStm.stmConstType = stmConstSelect
	s.sqlStm.selects = append(s.sqlStm.selects, columns...)
	return s
}

func (s *sqlBuilder) SelectDistinct(columns ...string) *sqlBuilder {
	s.sqlStm.distinct = true
	s.Select(columns...)
	return s
}

func (s *sqlBuilder) DeleteFrom(table string) *sqlBuilder {
	s.sqlStm.stmConstType = stmConstDelete
	s.sqlStm.tables = append(s.sqlStm.tables, table)
	return s
}

func (s *sqlBuilder) From(tables ...string) *sqlBuilder {
	s.sqlStm.tables = append(s.sqlStm.tables, tables...)
	return s
}

func (s *sqlBuilder) Join(joins ...string) *sqlBuilder {
	s.sqlStm.join = append(s.sqlStm.join, joins...)
	return s
}

func (s *sqlBuilder) InnerJoin(joins ...string) *sqlBuilder {
	s.sqlStm.innerJoin = append(s.sqlStm.innerJoin, joins...)
	return s
}

func (s *sqlBuilder) LeftOuterJoin(joins ...string) *sqlBuilder {
	s.sqlStm.leftOuterJoin = append(s.sqlStm.leftOuterJoin, joins...)
	return s
}

func (s *sqlBuilder) RightOuterJoin(joins ...string) *sqlBuilder {
	s.sqlStm.rightOuterJoin = append(s.sqlStm.rightOuterJoin, joins...)
	return s
}

func (s *sqlBuilder) OuterJoin(joins ...string) *sqlBuilder {
	s.sqlStm.outerJoin = append(s.sqlStm.outerJoin, joins...)
	return s
}

func (s *sqlBuilder) Where(conditions ...string) *sqlBuilder {
	sliceRefAppend(s.sqlStm.where, conditions...)
	s.sqlStm.lastList = s.sqlStm.where
	return s
}

func (s *sqlBuilder) Or() *sqlBuilder {
	sliceRefAppend(s.sqlStm.lastList, logicOR)
	return s
}

func (s *sqlBuilder) And() *sqlBuilder {
	sliceRefAppend(s.sqlStm.lastList, logicAND)
	return s
}

func (s *sqlBuilder) GroupBy(columns ...string) *sqlBuilder {
	s.sqlStm.groupBy = append(s.sqlStm.groupBy, columns...)
	return s
}

func (s *sqlBuilder) Having(conditions ...string) *sqlBuilder {
	s.sqlStm.having = append(s.sqlStm.having, conditions...)
	return s
}

func (s *sqlBuilder) OrderBy(columns ...string) *sqlBuilder {
	s.sqlStm.orderBy = append(s.sqlStm.orderBy, columns...)
	return s
}

func (s *sqlBuilder) String() string {
	return s.sqlStm.sql(&bytes.Buffer{})
}

func (s *sqlBuilder) Clear() *sqlBuilder {
	s.sqlStm = newStatement()
	return s
}

func sliceRefAppend(slice *[]string, val ...string) {
	*slice = append(*slice, val...)
}
