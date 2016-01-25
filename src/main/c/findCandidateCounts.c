#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct wordcount {
        char word[1000];
        int count;
        struct wordcount *next;
};
int WORDCOUNTLIMIT=1000;

struct candidate {
        char name[300];
        struct wordcount *words;
        int webPageMentions;
        int tempFoundFlag;
};

struct candidate trump, clinton, sanders, rubio, cruz, bush, omalley;
struct candidate *CANDIDATE_LIST[7] = {&trump, &clinton, &sanders, &rubio, &cruz, &bush, &omalley};
int CANDIDATE_LIST_LEN = 1;
int TRUMP=0,
        CLINTON=1,
        SANDERS=2,
        RUBIO=3,
        CRUZ=4,
        BUSH=5,
        OMALLEY=6;


int foundtrump = 0,
        foundclinton = 0,
        foundrubio = 0,
        foundcruz = 0,
        foundbush = 0,
        foundsanders=0,
        foundomalley = 0;

int printCandidates() {
        struct candidate *cptr;
        struct wordcount *wcptr;
        int i, j;

        for (i=0; i<CANDIDATE_LIST_LEN; i++) {
                cptr = CANDIDATE_LIST[i];
                printf("(%s, %s, %i)\n", CANDIDATE_LIST[i]->name, CANDIDATE_LIST[i]->name, CANDIDATE_LIST[i]->webPageMentions);
                wcptr = CANDIDATE_LIST[i]->words;
                while (wcptr) {
                        printf("(%s, %s, %i)\n", CANDIDATE_LIST[i]->name, wcptr->word, wcptr->count);
                        wcptr=wcptr->next;
                }
        }

}

int addWord(char *wordtobeadded, struct wordcount **webpagewords) {
        struct wordcount *thisword, *webpagewordsptr;
        int matchfound = 0;


        if ((*webpagewords) == NULL) {
                (*webpagewords) =  (struct wordcount *) malloc(sizeof(struct wordcount));
                strncpy((*webpagewords)->word, wordtobeadded, WORDCOUNTLIMIT);
                (*webpagewords)->count = 1;
                (*webpagewords)->next = NULL;
                thisword = *webpagewords;
        } else {
                webpagewordsptr = *webpagewords;

                matchfound=0;
                while (webpagewordsptr != NULL) {
                if (match(webpagewordsptr->word, wordtobeadded) != -1) {
                                matchfound = 1;
                                webpagewordsptr->count++;
                                thisword = webpagewordsptr;
                                break;
                        }
                        webpagewordsptr = webpagewordsptr->next;
                }
                if (!matchfound) {
                        thisword = (struct wordcount *) malloc(sizeof(struct wordcount));
                        strncpy(thisword->word, wordtobeadded, WORDCOUNTLIMIT);
                        thisword->count=1;
                        thisword->next = (*webpagewords);
                        (*webpagewords) = thisword;
                }
        }
}


int match(char text[], char pattern[]) {
 int i, j, ptr;
 int textlen,
        patternlen=strlen(pattern),
        position=-1;

 textlen = strlen(text);
 if (patternlen == 0) return -1;
 for (i=0; i<textlen; i++) {
        ptr=i;
        position = ptr;

        for (j=0; j<patternlen; j++) {
                if (pattern[j] == text[ptr]) ptr++;
                else break;
        }
        if (j==patternlen) return position;
 }
 return -1;
}

int freeWebpagewords(struct wordcount **webpagewords) {
        struct wordcount *nextptr, *webpagewordsptr;

        webpagewordsptr = (*webpagewords);

        while (webpagewordsptr) {
                nextptr = webpagewordsptr->next;
                free(webpagewordsptr);
                webpagewordsptr = nextptr;
        }
        (*webpagewords) = NULL;
}

int freeCandidates() {

        int i;

        for (i=0; i<CANDIDATE_LIST_LEN; i++) {
                freeWebpagewords(&(CANDIDATE_LIST[i]->words));
                CANDIDATE_LIST[i]->words=NULL;
        }
}

int updateWordCount(struct wordcount **dest, struct wordcount *source) {

        struct wordcount *ptr;
        int i, count;

        ptr = source;
        while (ptr) {
                count = ptr->count;
                for (i=0; i<count; i++)
                        addWord(ptr->word, dest);
                ptr = ptr->next;
        }
}

int updateCandidateWords(struct wordcount *webpagewords) {
        struct wordcount *ptr;
        int i;

        for (i=0; i< CANDIDATE_LIST_LEN; i++) {
                if (CANDIDATE_LIST[i]->tempFoundFlag == 1)
                        updateWordCount(&(CANDIDATE_LIST[i]->words), webpagewords);
        }
}

int resetCandidates() {

	int i;

	for (i=0; i<CANDIDATE_LIST_LEN; i++) {
		freeWebpagewords(&(CANDIDATE_LIST[i]->words));
                CANDIDATE_LIST[i]->words=NULL;
                CANDIDATE_LIST[i]->webPageMentions=0;
                CANDIDATE_LIST[i]->tempFoundFlag = 0;
	}
 
}

int initcandidates() {

        int i;

        for (i=0; i<CANDIDATE_LIST_LEN; i++) {

                if (i == TRUMP) strcpy(CANDIDATE_LIST[i]->name, "Donald Trump");
                if (i == CLINTON) strcpy(CANDIDATE_LIST[i]->name, "Hilary Clinton");
                if (i == SANDERS) strcpy(CANDIDATE_LIST[i]->name, "Bernie Sanders");
                if (i == CRUZ) strcpy(CANDIDATE_LIST[i]->name, "Ted Cruz");
                if (i == RUBIO) strcpy(CANDIDATE_LIST[i]->name, "Marco Rubio");
                if (i == OMALLEY) strcpy(CANDIDATE_LIST[i]->name, "Martin O'Malley");
                if (i == BUSH) strcpy(CANDIDATE_LIST[i]->name, "Jeb Bush");

                CANDIDATE_LIST[i]->words=NULL;
                CANDIDATE_LIST[i]->webPageMentions=0;
                CANDIDATE_LIST[i]->tempFoundFlag = 0;
        }
}


void main() {
 int ptr, i, j, len;
 char tempstring[10000];
 int tempstringlen=0;
 struct wordcount *webpagewords = NULL;
 char line[10000];
 int newHeader = 0;
 FILE *fp;
 int mycount=0;
 int reportFlag=0;

/* fp = fopen("./sampleCC.txt/part-00000", "r"); */
/* fp = fopen("./smallSampleCC.txt", "r"); */

 initcandidates();
 while (fgets(line, sizeof(line), stdin)) {
 mycount++;
 len = strlen(line);
 if (match(line, "WARC/1.0") != -1) {
        /* Found new header and reset counts */
        updateCandidateWords(webpagewords);
	for (i=0; i<CANDIDATE_LIST_LEN; i++) {
		if (CANDIDATE_LIST[i]->tempFoundFlag == 1) reportFlag = 1;
	}
	if (reportFlag == 1) {
		printCandidates();
		resetCandidates();
		reportFlag = 0;
	}
/*        for (i=0; i<CANDIDATE_LIST_LEN; i++) CANDIDATE_LIST[i]->tempFoundFlag = 0; */
        freeWebpagewords(&webpagewords);
 } else if (match(line, "WARC-") == -1) {

        for( i=0; i<CANDIDATE_LIST_LEN; i++) {
                if (match(line, CANDIDATE_LIST[i]->name) != -1) {
                        CANDIDATE_LIST[i]->tempFoundFlag = 1;
                        (CANDIDATE_LIST[i]->webPageMentions)++;
                }
        }

        for (i=0; i< len; i++) {
                for (j=0; line[i] != ' ' && line[i] != '\0' && i<len; j++,i++) {
                        tempstring[j] = line[i];
                }
                tempstring[j] = '\0';
               if (j>0) addWord(tempstring, &webpagewords);
        }
 }
}
printCandidates();
freeCandidates();
}

