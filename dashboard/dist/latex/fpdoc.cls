%% fpdoc — the A4 sheet under the Faceplate document classes (fpcv, fpletter),
%% and a class of its own for anything else on paper. It is the page chassis:
%% a top bar, a lead, sections with a label column, rows, a footer. Twelve
%% columns; the first three are the label column. Nothing is boxed: a section
%% opens with a rule and its ruler ticks, and four registration marks stand
%% where a frame would. The dial (intensity) moves colour, line weight, type
%% size and room, and nothing else, so a design change here reaches every
%% document class. Hand-written on top of faceplate.sty. Compile with LuaLaTeX.
%%   \documentclass[intensity=02]{fpdoc}   \kind{Report} \name{…} \updated{…}
%%   \makelead{Kicker}{Title}{One line that says what this is.}
%%   \fpsection{Title} … \fprow{key}{text} … \fpsection[ink]{Title} …
\NeedsTeXFormat{LaTeX2e}[2022/11/01]
\ProvidesClass{fpdoc}[2026/10/11 Faceplate A4 sheet]
\def\fpdoc@intensity{02}
\def\fpdoc@level{personal}
\DeclareKeys[fpdoc]{ intensity.store = \fpdoc@intensity, level.store = \fpdoc@level }
\ProcessKeyOptions[fpdoc]
\PassOptionsToPackage{intensity=\fpdoc@intensity, level=\fpdoc@level}{faceplate}
\LoadClass[a4paper]{article}
\RequirePackage{faceplate}
\RequirePackage{geometry}
\RequirePackage{graphicx}
\RequirePackage{ragged2e}
\RequirePackage{lastpage}
\RequirePackage{refcount}
\RequirePackage[hidelinks]{hyperref}

%% ---------------------------------------------------------------- page
%% Margins are step 7 all round. The top bar stands inside the top margin, the
%% footer inside the bottom one, so the text area starts and ends on a rule.
\geometry{a4paper, left=\fpspace{7}, right=\fpspace{7}, top=\fpspace{7}, bottom=\fpspace{7},
  headheight=\fpspace{4}, headsep=\fpspace{3}, footskip=\fpspace{5}}

%% Every size is a step of the closed scale; the old names map onto it.
\renewcommand\normalsize{\fpsize{base}}
\renewcommand\small{\fpsize{sm}}
\renewcommand\footnotesize{\fpsize{xs}}
\renewcommand\scriptsize{\fpsize{xxs}}
\renewcommand\tiny{\fpsize{xxs}}
\renewcommand\large{\fpsize{h3}}
\renewcommand\Large{\fpsize{h2}}
\renewcommand\LARGE{\fpsize{h1}}
\renewcommand\huge{\fpsize{h1}}
\renewcommand\Huge{\fpsize{h1}}
\normalsize
\setlength\parindent{0pt}
\setlength\parskip{0pt}
\AtBeginDocument{\color{fpCtxBody}\RaggedRight}

%% ---------------------------------------------------------------- grid
%% Twelve columns. On the full sheet the label column (3) holds the index
%% square and the section title, and the body (9) holds everything else; a
%% row inside the body gives its first two columns to a key. A half sheet
%% (six columns) is narrow: there the label becomes a line above its body.
\newlength\fpdoc@gutter \setlength\fpdoc@gutter{\fpspace{3}}
\newlength\fpdoc@col
\newlength\fpdoc@half
\newlength\fpdoc@width      % the sheet in use: the text width, or a half
\newlength\fpdoc@label      % the label column, 0 on a narrow sheet
\newlength\fpdoc@indent     % where the body starts
\newlength\fpdoc@body
\newlength\fpdoc@key
\newlength\fpdoc@text
\newlength\fpdoc@bodystart  % the body's left edge on the full sheet, for the top bar and the footer
\newif\iffpdoc@narrow
\newcommand\fpdoc@sheet[2]{% {<width>}{wide|narrow}
  \setlength\fpdoc@width{#1}%
  \setlength\fpdoc@key{\dimexpr2\fpdoc@col+\fpdoc@gutter\relax}%
  \csname fpdoc@sheet@#2\endcsname
  \setlength\fpdoc@body{\dimexpr\fpdoc@width-\fpdoc@indent\relax}%
  \setlength\fpdoc@text{\dimexpr\fpdoc@body-\fpdoc@key-\fpdoc@gutter\relax}}
\def\fpdoc@sheet@wide{\fpdoc@narrowfalse
  \setlength\fpdoc@label{\dimexpr3\fpdoc@col+2\fpdoc@gutter\relax}%
  \setlength\fpdoc@indent{\dimexpr\fpdoc@label+\fpdoc@gutter\relax}}
\def\fpdoc@sheet@narrow{\fpdoc@narrowtrue
  \setlength\fpdoc@label{\z@}\setlength\fpdoc@indent{\z@}}
\AtBeginDocument{%
  \setlength\fpdoc@col{\dimexpr(\textwidth-11\fpdoc@gutter)/12\relax}%
  \setlength\fpdoc@half{\dimexpr6\fpdoc@col+5\fpdoc@gutter\relax}%
  \fpdoc@sheet{\textwidth}{wide}%
  \setlength\fpdoc@bodystart{\fpdoc@indent}}

%% The width of the body, for a figure that should fill it. Text and figures
%% after \fpsection start at the body's left edge by themselves.
\newcommand\fpbodywidth{\fpdoc@body}

%% ---------------------------------------------------------- primitives
%% The vertical rhythm is explicit. Every part is one box placed without
%% interline glue; text boxes carry their own struts, so a row is always a
%% whole number of lines and the spaces between parts are steps of the scale.
\newcommand\fpdoc@put[1]{\par\begingroup
  \parskip\z@ \leftskip\z@ \rightskip\z@ \parfillskip\z@ \parindent\z@
  \nointerlineskip\noindent\hbox to\fpdoc@width{#1\hss}\par\endgroup\nointerlineskip}
%% What a column of text starts with. Ragged right; a line may fall short by
%% any amount, because without hyphenation TeX would otherwise let it stick out.
\newcommand\fpdoc@par{\parskip\z@ \leftskip\z@ \parindent\z@ \RaggedRight \tolerance\@M \hbadness\@M
  \normalsize \color{fpCtxBody}}
\newcommand\fpdoc@meta[1]{\leavevmode{\ttfamily\fpsize{xs}\color{fpCtxBody}#1}}
\newcommand\fpdoc@dot{\hspace{.45em}{\color{fpCtxMuted}·}\hspace{.45em}}
\newcommand\fpdoc@ifempty[3]{\if\relax\detokenize{#1}\relax#2\else#3\fi}
%% a hairline across the body, in the rule colour of the field it is on
\newcommand\fpdoc@hairline{\par\nointerlineskip
  \moveright\fpdoc@indent\vbox{{\color{fpCtxRule}\hrule height\fphairline width\fpdoc@body}}\nointerlineskip}
%% break the page here unless <length> still fits on it
\newcommand\fpdoc@need[1]{\par\penalty\z@
  \begingroup\dimen@=\dimexpr\pagegoal-\pagetotal\relax
  \ifdim\dimen@<\dimexpr#1\relax \ifdim\dimen@>\z@ \vfil\break \fi\fi\endgroup}

%% ------------------------------------------------------------ sections
%% \fpsection[<kind>]{<title>} opens a section: room, the rule with its ticks,
%% then the label. On the full sheet the label (index square, title) hangs in
%% the label column and the body runs beside it, across page breaks if it has
%% to. On a narrow sheet the label is a line: square, then title. The kind is
%% sage (the work itself), ink (public and dated) or terra (made on the side).
%% \fpsection* leaves the index out.
\newcounter{fpsection}
\newbox\fpdoc@labelbox
\newdimen\fpdoc@labelht
\newdimen\fpdoc@secstart
\newcount\fpdoc@secpage
\newcount\fpdoc@shipouts
\AddToHook{shipout/before}{\global\advance\fpdoc@shipouts\@ne}
\newif\iffpdoc@insec
\newif\iffpdoc@noroom
\def\fpdoc@last{n}                  % what stands above in this section: nothing, an entry, an item
\newcommand\fpdoc@title[1]{{\fpheadingfont\fpsize{title}\color{fpCtxInk}#1}}
\newcommand\fpdoc@secrule{%
  \ifnum\fpintensity<4 \fpdoc@put{\fptickrule[\iffpdoc@narrow 6\else 12\fi]{\fpdoc@width}{\fpdoc@gutter}}\fi}

\NewDocumentCommand\fpsection{s O{sage} m}{%
  \fpdoc@endsection
  \fpkind{#2}%
  \IfBooleanF{#1}{\stepcounter{fpsection}}%
  \iffpdoc@narrow\else
    \setbox\fpdoc@labelbox\vbox{\hsize\fpdoc@label \fpdoc@par
      \IfBooleanF{#1}{\noindent\fpindex{\two@digits{\value{fpsection}}}\par\nointerlineskip\vskip\fpspace{2}}%
      \noindent\fpdoc@title{#3}\par}%
    \global\fpdoc@labelht=\dimexpr\ht\fpdoc@labelbox+\dp\fpdoc@labelbox\relax
  \fi
  \iffpdoc@noroom \global\fpdoc@noroomfalse
  \else
    \par\addvspace{\fp@dial@room}%
    \iffpdoc@narrow\else\fpdoc@need{\dimexpr\fpdoc@labelht+\fpspace{5}\relax}\fi
  \fi
  \fpdoc@secrule
  \nobreak\vskip\fpspace{3}\nobreak
  \iffpdoc@narrow
    \fpdoc@put{\IfBooleanF{#1}{\fpindex{\two@digits{\value{fpsection}}}\hskip\fpspace{2}}%
      \setbox\fp@box\hbox{\fpdoc@title{#3}}%
      \raise\dimexpr(\fp@dial@indexSize-\ht\fp@box)/2\relax\box\fp@box}%
    \nobreak\vskip\fpspace{2}\nobreak
  \else
    \nointerlineskip\vbox to\z@{\box\fpdoc@labelbox\vss}\nointerlineskip
    \nobreak
    \global\fpdoc@secstart=\pagetotal \global\fpdoc@secpage=\fpdoc@shipouts
  \fi
  \global\fpdoc@insectrue \gdef\fpdoc@last{n}%
  \leftskip\fpdoc@indent \color{fpCtxBody}}

%% A section is at least as tall as its label.
\newcommand\fpdoc@endsection{%
  \iffpdoc@insec
    \par\penalty\z@
    \iffpdoc@narrow\else
      \ifnum\fpdoc@secpage=\fpdoc@shipouts
        \fp@dim=\dimexpr\fpdoc@secstart+\fpdoc@labelht-\pagetotal\relax
        \ifdim\fp@dim>\z@ \vskip\fp@dim \penalty\z@ \fi     % the penalty keeps \addvspace from swallowing it
      \fi
    \fi
    \global\fpdoc@insecfalse \leftskip\z@
  \fi}
\AtEndDocument{\fpdoc@endsection}

%% ---------------------------------------------------------------- rows
%% Parts follow each other inside a section: an entry opens with a hairline
%% unless it is the first, an item sits close under what it belongs to.
\newcommand\fpdoc@sep[1]{\par
  \if e#1%
    \if n\fpdoc@last\else
      \vskip\fp@dial@row \fpdoc@hairline \nobreak\vskip\fp@dial@row \nobreak
    \fi
  \else
    \if e\fpdoc@last \vskip\fpspace{2}\fi
    \if i\fpdoc@last \vskip\fpspace{1}\fi
  \fi}
%% a key and its text: the key in the first two body columns, mono
\newcommand\fpdoc@keyrow[2]{%
  \fpdoc@put{\hskip\fpdoc@indent
    \vtop{\hsize\fpdoc@key \fpdoc@par \strut\fpdoc@meta{#1}\par}%
    \hskip\fpdoc@gutter
    \vtop{\hsize\fpdoc@text \fpdoc@par \strut#2\strut\par}}}
%% text across the whole body
\newcommand\fpdoc@wholerow[1]{%
  \fpdoc@put{\hskip\fpdoc@indent\vtop{\hsize\fpdoc@body \fpdoc@par \strut#1\strut\par}}}
%% a label in the label column, its content in the body (full sheet only)
\newcommand\fpdoc@labelrow[2]{%
  \fpdoc@put{%
    \vtop{\hsize\fpdoc@label \fpdoc@par \strut#1\par}%
    \hskip\fpdoc@gutter
    \vtop{\hsize\fpdoc@body \fpdoc@par \strut#2\strut\par}}}

%% \fprow{<key>}{<text>} — a dated row: news, talks, anything with a date or a
%% key in front. Rows are parted by hairlines.
\newcommand\fprow[2]{\fpdoc@sep{e}\fpdoc@keyrow{#1}{{\color{fpCtxInk}#2}}\gdef\fpdoc@last{e}}
%% \fpitem{<key>}{<text>} — the same without a line, closer together. An empty
%% key lets the text take the whole body.
\newcommand\fpitem[2]{\fpdoc@sep{i}%
  \fpdoc@ifempty{#1}{\fpdoc@wholerow{#2}}{\fpdoc@keyrow{#1}{#2}}\gdef\fpdoc@last{i}}

%% ----------------------------------------------------------------- lead
%% Who or what this document is. Label column: a square photo held by four
%% marks, or the contact lines. Body: a label, the display line, a lede, text.
%% From 02 the body stands on a field that runs from the edge of the label
%% column to the edge of the paper; at 04 the field takes the whole top of
%% the page, top bar included.
\newcommand\name[1]{\def\fpdoc@name{#1}}            \name{}
\newcommand\role[1]{\def\fpdoc@role{#1}}            \role{}
\newcommand\photo[1]{\def\fpdoc@photofile{#1}}      \photo{}
\newcommand\contact[1]{\def\fpdoc@contact{#1}}      \contact{}
\newcommand\updated[1]{\def\fpdoc@updated{#1}}      \updated{}
\newcommand\kind[1]{\def\fpdoc@kind{#1}}            \kind{Document}

\newbox\fpdoc@leadbox
\newdimen\fpdoc@bandht
\newcommand\fpdoc@leadctx{%
  \colorlet{fpCtxBg}{fpLeadFill}\colorlet{fpCtxInk}{fpLeadInk}\colorlet{fpCtxBody}{fpLeadBody}%
  \colorlet{fpCtxMuted}{fpLeadBody}\colorlet{fpCtxAccent}{fpLeadAccent}}
\newcommand\fpdoc@lead[2]{% {<label column>}{<body>}
  \par\thispagestyle{fpdoclead}\begingroup
  \ifnum\fpintensity>3 \fpdoc@leadctx \colorlet{fpCtxMark}{fpLeadInk}\colorlet{fpLineColor}{fpLeadInk}\fi
  \fpdoc@put{\fptickrule{\fpdoc@width}{\fpdoc@gutter}}\nobreak
  \setbox\fpdoc@leadbox\hbox{%
    \hbox to\fpdoc@label{\vtop{\hsize\fpdoc@label \fpdoc@par \vskip\fp@dial@leadTop #1\par\vskip\fp@dial@leadBottom}\hss}%
    \hskip\fpdoc@gutter
    \vtop{\hsize\fpdoc@body \fpdoc@leadctx \fpdoc@par \vskip\fp@dial@leadTop #2\par\vskip\fp@dial@leadBottom}}%
  \ifnum\fpintensity>3
    \global\fpdoc@bandht=\dimexpr\fpspace{7}+\fpline+\dp\fpdoc@leadbox\relax
    \AddToHookNext{shipout/background}{\put(0pt,-\fpdoc@bandht){\color{fpLeadFill}\rule{\paperwidth}{\fpdoc@bandht}}}%
  \fi
  \fpdoc@put{%
    \ifnum\fpintensity<4 \rlap{\kern\fpdoc@label\color{fpLeadFill}\rule[-\dp\fpdoc@leadbox]{\paperwidth}{\dp\fpdoc@leadbox}}\fi
    \box\fpdoc@leadbox}%
  \endgroup
  \hypersetup{pdftitle={\fpdoc@name\space - \fpdoc@kind}, pdfauthor={\fpdoc@name}}%
  \global\fpdoc@noroomtrue}

%% the parts of a lead, in the order they are read
\newcommand\fpdoc@leadlabel[1]{\fpdoc@ifempty{#1}{}{\noindent\fplabel[fpCtxBody]{#1}\par\vskip\fpspace{1}}}
\newcommand\fpdoc@leaddisplay[1]{\noindent{\fpdisplayfont\fpsize{display}\color{fpCtxInk}#1\par}}
\newcommand\fpdoc@leadlede[1]{\fpdoc@ifempty{#1}{}{\vskip\fpspace{2}\noindent{\fpsemibold\fpsize{lede}\color{fpCtxInk}#1\par}}}
\newcommand\fpdoc@leadtext[1]{\fpdoc@ifempty{#1}{}{\vskip\fpspace{2}\noindent#1\par}}
%% The square photo, inset so that its four marks stand on the column's edges.
\newcommand\fpdoc@portrait{%
  \ifx\fpdoc@photofile\@empty\else
    \noindent\hskip\fpspace{2}\fpreg[\fpspace{2}]{\includegraphics[width=\dimexpr\fpdoc@label-\fpspace{3}\relax,
      height=\dimexpr\fpdoc@label-\fpspace{3}\relax, keepaspectratio]{\fpdoc@photofile}}\par
  \fi}
%% Contact lines. In the label column they stack; under the text they run as a
%% row, and what links somewhere stands on an accent keyline.
\newcommand\fpdoc@stack{%
  \ifx\fpdoc@contact\@empty\else{\ttfamily\fpsize{xs}\color{fpCtxBody}\lineskiplimit=-\maxdimen
    \noindent\fpdoc@contact\par}\fi}
\newcommand\fpdoc@underline[1]{\setbox\fp@box\hbox{#1}%
  \rlap{\color{fpCtxAccent}\rule[-\fpspace{1}]{\wd\fp@box}{\fpkeyline}}\box\fp@box}
\newcommand\fpdoc@links{%
  \ifx\fpdoc@contact\@empty\else{\ttfamily\fpsize{xs}\color{fpCtxInk}%
    \baselineskip=\dimexpr\baselineskip+\fpspace{2}\relax
    \let\fpdoc@href\href \def\href##1##2{\fpdoc@href{##1}{\fpdoc@underline{##2}}}%
    \def\\{\hskip\fpspace{4}\penalty\z@}%
    \vskip\fpspace{3}\noindent\fpdoc@contact\par}\fi}

%% \makelead[<text>]{<label>}{<title>}{<lede>} — the lead of a plain document.
%% The label column takes the photo, or the contact lines, or else the label.
\newcommand\makelead[4][]{%
  \ifx\fpdoc@photofile\@empty
    \ifx\fpdoc@contact\@empty
      \fpdoc@lead{\noindent\strut\fplabel[fpCtxBody]{#2}}{\fpdoc@leaddisplay{#3}\fpdoc@leadlede{#4}\fpdoc@leadtext{#1}}%
    \else
      \fpdoc@lead{\fpdoc@stack}{\fpdoc@leadlabel{#2}\fpdoc@leaddisplay{#3}\fpdoc@leadlede{#4}\fpdoc@leadtext{#1}}%
    \fi
  \else
    \fpdoc@lead{\fpdoc@portrait}{\fpdoc@leadlabel{#2}\fpdoc@leaddisplay{#3}\fpdoc@leadlede{#4}\fpdoc@leadtext{#1}\fpdoc@links}%
  \fi}

%% -------------------------------------------------- top bar and footer
%% The top bar: the mark, the kind of document, a date stamp. The footer: a
%% rule, the name, the page index. Both are mono and quiet, and both stand in
%% the margins. On the first page at 04 the top bar is part of the lead's field.
\newcommand\fpdoc@topbar[3]{% {<mark colour>}{<text colour>}{<stamp colour>}
  \hbox to\textwidth{%
    \setbox\fp@box\hbox{\fpmark[#1]{\fpspace{5}}}%
    \rlap{\lower\dimexpr(\ht\fp@box-.7ex)/2\relax\box\fp@box}%
    \hskip\fpdoc@bodystart
    \fplabel[#2]{\fpdoc@kind}\hfill
    \ifx\fpdoc@updated\@empty\else\fplabel[#3]{\fpdoc@updated}\fi}}
\newcommand\fpdoc@footer{%
  \vbox{\hsize\textwidth
    \ifnum\fpintensity<4 {\color{fpLineColor}\hrule height\fpline}\fi
    \vskip\fpspace{3}%
    \hbox to\textwidth{%
      \rlap{\fplabel[fpBody]{\fpdoc@name}}\hskip\fpdoc@bodystart
      \fplabel{\fpdoc@kind}\hfill
      \fplabel[fpBody]{\two@digits{\value{page}}\,/\,\two@digits{\getpagerefnumber{LastPage}}}}}}
\def\ps@fpdoc{%
  \def\@oddhead{\fpdoc@topbar{fpSage}{fpBody}{fpMuted}}\let\@evenhead\@oddhead
  \def\@oddfoot{\fpdoc@footer}\let\@evenfoot\@oddfoot}
\def\ps@fpdoclead{\ps@fpdoc
  \ifnum\fpintensity>3
    \def\@oddhead{\fpdoc@topbar{fpLeadAccent}{fpLeadBody}{fpLeadBody}}\let\@evenhead\@oddhead
  \fi}
\pagestyle{fpdoc}

\endinput
