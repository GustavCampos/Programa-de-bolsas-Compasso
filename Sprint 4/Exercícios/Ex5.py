import csv
from os.path import join, dirname, realpath


def main():
    location = join(dirname(realpath(__file__)), 'estudantes.csv')
    
    with open(location, 'r', encoding="utf8") as file:
        # Lendo linhas do csv
        students = [row for row in csv.reader(file)]
    
    normalized_students = {n[0]: list(map(int, n[1:])) for n in students}
        
    sort_by_student = sorted(normalized_students.items(), key=lambda n: n[0])
    
    top_3_grades_map = map(
        lambda n: (n[0], sorted(n[1], reverse=True)[:3]),
        sort_by_student
    )
    
    students_map = map(
        lambda n: (*n, round(sum(n[1]) / len(n[1]), 2)),
        top_3_grades_map
    )
    
    for student in students_map:
        print("Nome: {} Notas: {} Média: {}".format(*student))
    
main()
