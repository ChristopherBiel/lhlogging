%% fpcv — a CV in Faceplate. Personal level, intensity 01 unless you say otherwise, A4.
%% The sheet, the lead, sections, rows and the footer come from fpdoc; this adds
%% the CV's own parts.
%%   \documentclass{fpcv}                 01 Line
%%   \documentclass[intensity=02]{fpcv}   02 Tint: ink keylines, a tinted lead, tinted index squares
%% Rules: guidelines/applications/cv.md · How to use: guidelines/media/latex.md
\NeedsTeXFormat{LaTeX2e}[2022/11/01]
\ProvidesClass{fpcv}[2026/10/11 Faceplate CV]
\def\fpcv@intensity{01}
\DeclareKeys[fpcv]{ intensity.store = \fpcv@intensity }
\ProcessKeyOptions[fpcv]
\PassOptionsToClass{intensity=\fpcv@intensity, level=personal}{fpdoc}
\LoadClass{fpdoc}
\kind{Curriculum vitae}
%% Ragged right, and no hyphenation: a CV is scanned, not read through.
\hyphenpenalty=10000 \exhyphenpenalty=10000

%% \makecvheader{<profile>} — the lead: the photo in the label column, held by
%% four marks; the role, the name, the profile and the contact lines beside it.
%% Crop the photo square. Without one, the contact lines take the label column.
\newcommand\makecvheader[1]{%
  \fpdoc@lead{\ifx\fpdoc@photofile\@empty\fpdoc@stack\else\fpdoc@portrait\fi}{%
    \fpdoc@leadlabel{\fpdoc@role}\fpdoc@leaddisplay{\fpdoc@name}\fpdoc@leadtext{#1}%
    \ifx\fpdoc@photofile\@empty\else\fpdoc@links\fi}}

%% ------------------------------------------------------------ sections
%% \cvsection[<kind>]{<title>} — an index square and the title in the label
%% column, under a rule with its ruler ticks. The kind is sage by default;
%% ink is for what is public and dated, terra for what is made on the side.
\NewDocumentCommand\cvsection{O{sage} m}{\fpsection[#1]{#2}}

%% Short sections in pairs, side by side: six columns each. A half is a narrow
%% sheet, so its label becomes a line above its body: square, then title.
%%   \begin{cvcolumns} \cvsection{A} … \nextcvcolumn \cvsection{B} … \end{cvcolumns}
\newcommand\fpcv@enterhalf{\fpdoc@sheet{\fpdoc@half}{narrow}\global\fpdoc@noroomtrue \fpdoc@par}
\newenvironment{cvcolumns}{%
  \fpdoc@endsection
  \par\addvspace{\fp@dial@room}\fpdoc@need{\fpspace{8}}%
  \begingroup\parskip\z@ \leftskip\z@ \nointerlineskip\noindent
  \begin{minipage}[t]{\fpdoc@half}\fpcv@enterhalf}{%
  \fpdoc@endsection\end{minipage}\par\endgroup}
\newcommand\nextcvcolumn{%
  \fpdoc@endsection\end{minipage}\hspace{\fpdoc@gutter}%
  \begin{minipage}[t]{\fpdoc@half}\fpcv@enterhalf}

%% ------------------------------------------------------------- entries
%% \cventry{<when>}{<title>}{<where>}{<details>} — any argument may be empty.
%% The title is Bold ink and the place follows a muted dot; the date stands at
%% the right end of that line, in mono, so the dates of a section read down one
%% edge. The details run below across the whole body: on paper the text needs
%% all nine columns, or a two-page CV becomes three. Entries are parted by
%% hairlines.
\newcommand\cventry[4]{%
  \fpdoc@sep{e}%
  \fpdoc@ifempty{#1}{%
    \fpdoc@wholerow{{\color{fpCtxInk}\bfseries #2}\fpdoc@ifempty{#3}{}{\fpdoc@dot{\color{fpCtxInk}#3}}}%
  }{%
    \fpdoc@put{\hskip\fpdoc@indent
      \vtop{\hsize\fpdoc@text \fpdoc@par
        \strut{\color{fpCtxInk}\bfseries #2}\fpdoc@ifempty{#3}{}{\fpdoc@dot{\color{fpCtxInk}#3}}\strut\par}%
      \hskip\fpdoc@gutter
      \vtop{\hsize\fpdoc@key \fpdoc@par \raggedleft \strut\fpdoc@meta{#1}\par}}%
  }%
  \fpdoc@ifempty{#4}{}{\nobreak\fpdoc@wholerow{#4}}%
  \gdef\fpdoc@last{e}}

%% \cvitem{<label>}{<text>} — a single row without a line, closer together.
%% With an empty label the text takes the whole body.
\newcommand\cvitem[2]{\fpitem{#1}{#2}}

%% \cvlevel{<n>} — n of five square pips, sage on signature grey.
\newcommand\cvlevel[1]{{%
  \count@=\z@
  \loop\ifnum\count@<5 \advance\count@\@ne
    \ifnum\count@>#1\relax\textcolor{fpGrey}{\rule{.62em}{.62em}}%
    \else\textcolor{fpSage}{\rule{.62em}{.62em}}\fi
    \ifnum\count@<5 \hspace{.22em}\fi
  \repeat}}

\endinput
