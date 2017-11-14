list.of.packages <- c('ggplot2',
                      'grid',
                      'gridExtra',
                      'quantreg',
                      'reshape2',
                      'scales',
                      'extrafont',
                      'ggthemes',
                      'plyr',
                      'RColorBrewer',
                      'Hmisc')
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if (length(new.packages)) install.packages(new.packages,
                                           repos="http://cran.rstudio.com/")

library(ggplot2)
library(grid)
library(gridExtra)
library(quantreg)
library(reshape2)
library(scales)
library(extrafont)
library(ggthemes)
library(plyr)
library(RColorBrewer)
library(ggthemes)


myTheme <- theme_bw(base_family = '') +
  theme(#panel.grid.major = element_line(color='lightgrey',
        #                                linetype='dashed'),
        #panel.grid.minor = element_line(color='lightgrey',
        #                                linetype ='dotted'),
        #axis.ticks.length = unit(-0.2, 'cm'),
        text = element_text(size = rel(3.1)),
        legend.text = element_text(size=rel(2.4)),
        legend.title = element_text(face='plain', size=rel(2.4)),
        axis.text = element_text(size=rel(1.2), family='', color='black', face='bold'),
        #axis.line.x = element_line(color = 'black'),
        #axis.line.y = element_line(color = 'black'),
        axis.title.x = element_text(vjust = -0.3, size=rel(4.4), family='', color='black', face='bold'),
        axis.title.y = element_text(vjust = 0.8, size=rel(4.4), family='', color='black', face='bold'),
        legend.background = element_blank(), 
        legend.key = element_blank(), 
        #legend.spacing = unit(0, 'cm'),
        #legend.title = element_text(face="plain"),
        panel.background = element_blank(), 
        panel.border = element_blank(),
        panel.grid = element_blank(),
        plot.background = element_blank(),
        strip.background = element_blank(),
        strip.text = element_text(size=rel(0.85)),
        legend.margin = margin()
  )

is.extrafont.installed <- function(){
  if(is.element("extrafont", installed.packages()[,1])){
    library(extrafont)
    # probably need something here to run font_import()
    return(T)
  }else{
    warning("Library extrafont installed; using system sans/serif libraries as fallback fonts. 
            To enable full font support, run: 
            install.packages('extrafont') 
            font_import()")
    return(F)
  }
}

base_font_family_tufte <- function(){
  if(is.extrafont.installed()){
    library(extrafont)
    loadfonts(quiet=T)
    tuftefont <- choose_font(c('Times New Roman',
                               "Gill Sans MT",
                               "Gill Sans",
                               "GillSans",
                               "Verdana",
                               "serif"), quiet = T)  
  }else{
    tuftefont <- "serif"
  }
  return(tuftefont)
}

theme_tufte_revised <- function(base_size = 11, base_family = base_font_family_tufte(), ticks = TRUE) {
  
  ret <- theme_bw(base_family = base_family, base_size = base_size) + 
    theme(
      axis.line = element_line(color = 'black'),
      axis.title.x = element_text(vjust = -0.3), 
      axis.title.y = element_text(vjust = 0.8),
      legend.background = element_blank(), 
      legend.key = element_blank(), 
      legend.title = element_text(face="plain"),
      panel.background = element_blank(), 
      panel.border = element_blank(),
      panel.grid = element_blank(),
      plot.background = element_blank(),
      strip.background = element_blank()
    )
  
  if (!ticks) {
    ret <- ret + theme(axis.ticks = element_blank())
  }
  
  ret
}

bigFonts <- myTheme +
  theme(text=element_text(size=rel(4)),
        legend.text=element_text(size=rel(3)),
        legend.title=element_text(size=rel(3)))


multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  library(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}
